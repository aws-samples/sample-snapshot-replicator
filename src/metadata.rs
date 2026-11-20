// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

//! Snapshot metadata operations
//!
//! This module handles retrieving snapshot metadata and listing snapshot blocks
//! using AWS EC2 and EBS Direct APIs.

use crate::block_copier::{BlockInfo, ChangedBlock, EBS_BLOCK_SIZE};
use thiserror::Error;

/// Maximum blocks returned per `ListSnapshotBlocks` / `ListChangedBlocks` page.
///
/// The service default is 100; requesting the maximum (10,000) minimises the
/// number of list API calls on large snapshots.
const LIST_BLOCKS_MAX_RESULTS: i32 = 10_000;

/// Metadata operation errors
#[derive(Debug, Error)]
pub enum MetadataError {
    #[error("Snapshot {snapshot_id} not found in region {region}")]
    NotFound { snapshot_id: String, region: String },

    #[error("Snapshot {snapshot_id} not ready (state: {state})")]
    NotReady { snapshot_id: String, state: String },

    #[error(
        "Snapshot {snapshot_id} is in the `archive` storage tier, which the EBS \
         direct APIs cannot read. Restore it to the standard tier (for example \
         with `aws ec2 restore-snapshot-tier`) and wait for the restore to \
         complete before replicating."
    )]
    Archived { snapshot_id: String },

    #[error("Snapshot {snapshot_id} response is missing the `{field}` field")]
    MalformedResponse { snapshot_id: String, field: String },

    #[error("Failed to describe snapshot {snapshot_id}")]
    DescribeFailed {
        snapshot_id: String,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[error("Failed to list blocks for snapshot {snapshot_id}")]
    ListFailed {
        snapshot_id: String,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

/// Snapshot lifecycle state
#[derive(Debug, Clone, PartialEq)]
pub enum SnapshotState {
    Pending,
    Completed,
    Error,
}

/// Snapshot metadata information
#[derive(Debug, Clone)]
pub struct SnapshotMetadata {
    /// Snapshot description
    pub description: Option<String>,
    /// AWS resource tags
    pub tags: Vec<aws_sdk_ec2::types::Tag>,
    /// Volume size in GiB
    pub volume_size: i64,
    /// Block size in bytes (always 524288 for EBS)
    pub block_size: i32,
    /// Current snapshot state
    pub state: SnapshotState,
}

/// Manager for snapshot metadata operations
#[derive(Debug)]
pub struct MetadataManager {
    /// EC2 client for source partition
    source_client: aws_sdk_ec2::Client,
    /// EBS client for source partition (for block operations)
    source_ebs_client: aws_sdk_ebs::Client,
}

impl MetadataManager {
    /// Create a new MetadataManager
    ///
    /// # Arguments
    ///
    /// * `source_client` - EC2 client for source partition
    /// * `source_ebs_client` - EBS client for source partition
    ///
    /// # Returns
    ///
    /// A new MetadataManager instance
    pub fn new(source_client: aws_sdk_ec2::Client, source_ebs_client: aws_sdk_ebs::Client) -> Self {
        Self {
            source_client,
            source_ebs_client,
        }
    }

    /// Retrieve and validate snapshot metadata
    ///
    /// # Arguments
    ///
    /// * `snapshot_id` - The snapshot ID to validate
    ///
    /// # Returns
    ///
    /// * `Ok(SnapshotMetadata)` - Snapshot metadata if valid and completed
    /// * `Err(MetadataError)` - Error if snapshot not found, not ready, or API error
    ///
    /// # Errors
    ///
    /// * `MetadataError::NotFound` - Snapshot does not exist
    /// * `MetadataError::NotReady` - Snapshot is not in completed state
    /// * `MetadataError::DescribeFailed` - AWS API error occurred
    pub async fn get_snapshot_metadata(
        &self,
        snapshot_id: &str,
    ) -> Result<SnapshotMetadata, MetadataError> {
        // Call DescribeSnapshots API
        let response = self
            .source_client
            .describe_snapshots()
            .snapshot_ids(snapshot_id)
            .send()
            .await
            .map_err(|e| MetadataError::DescribeFailed {
                snapshot_id: snapshot_id.to_string(),
                source: Box::new(e),
            })?;

        // Check if snapshot exists
        let snapshots = response.snapshots();
        if snapshots.is_empty() {
            let region = self
                .source_client
                .config()
                .region()
                .map(|r| r.as_ref().to_string())
                .unwrap_or_else(|| "unknown".to_string());
            return Err(MetadataError::NotFound {
                snapshot_id: snapshot_id.to_string(),
                region,
            });
        }

        // Extract first snapshot
        let snapshot = &snapshots[0];

        // Map AWS state to our state enum
        let aws_state = snapshot.state();
        let our_state = match aws_state {
            Some(aws_sdk_ec2::types::SnapshotState::Completed) => SnapshotState::Completed,
            Some(aws_sdk_ec2::types::SnapshotState::Pending) => SnapshotState::Pending,
            Some(aws_sdk_ec2::types::SnapshotState::Error) => SnapshotState::Error,
            _ => SnapshotState::Error,
        };

        // Validate state is completed
        if our_state != SnapshotState::Completed {
            return Err(MetadataError::NotReady {
                snapshot_id: snapshot_id.to_string(),
                state: format!("{:?}", our_state),
            });
        }

        // Archived snapshots report `completed` but cannot be read by the EBS
        // direct APIs; `ListSnapshotBlocks` would later fail with an opaque
        // error. Reject them up front with an actionable message.
        if snapshot.storage_tier() == Some(&aws_sdk_ec2::types::StorageTier::Archive) {
            return Err(MetadataError::Archived {
                snapshot_id: snapshot_id.to_string(),
            });
        }

        // Extract metadata fields
        let description = snapshot.description().map(|s| s.to_string());
        let tags = snapshot.tags().to_vec();
        // A completed snapshot always has a volume size; a missing value means a
        // malformed response, not a zero-sized volume, so fail rather than
        // fabricate 0.
        let volume_size =
            snapshot
                .volume_size()
                .ok_or_else(|| MetadataError::MalformedResponse {
                    snapshot_id: snapshot_id.to_string(),
                    field: "volume_size".to_string(),
                })? as i64;

        // Create and return metadata
        Ok(SnapshotMetadata {
            description,
            tags,
            volume_size,
            block_size: EBS_BLOCK_SIZE,
            state: our_state,
        })
    }

    /// List all data blocks in a snapshot
    ///
    /// # Arguments
    ///
    /// * `snapshot_id` - The snapshot ID to list blocks from
    ///
    /// # Returns
    ///
    /// * `Ok(Vec<BlockInfo>)` - Vector of all data blocks in the snapshot
    /// * `Err(MetadataError)` - Error if API call fails
    ///
    /// # Errors
    ///
    /// * `MetadataError::ListFailed` - AWS API error occurred
    ///
    /// # Notes
    ///
    /// This method handles pagination automatically, collecting all blocks
    /// across multiple API calls if the snapshot has more than 10,000 blocks.
    /// The EBS API automatically excludes sparse blocks (blocks with no data).
    pub async fn list_snapshot_blocks(
        &self,
        snapshot_id: &str,
    ) -> Result<Vec<BlockInfo>, MetadataError> {
        let mut all_blocks = Vec::new();
        let mut next_token: Option<String> = None;

        loop {
            // Build request with snapshot ID
            let mut request = self
                .source_ebs_client
                .list_snapshot_blocks()
                .snapshot_id(snapshot_id)
                .max_results(LIST_BLOCKS_MAX_RESULTS);

            // Add next token for pagination if present
            if let Some(token) = &next_token {
                request = request.next_token(token);
            }

            // Send request and await response
            let response = request
                .send()
                .await
                .map_err(|e| MetadataError::ListFailed {
                    snapshot_id: snapshot_id.to_string(),
                    source: Box::new(e),
                })?;

            // Collect blocks from this page. A block with no index or token is a
            // malformed response, not block 0 with an empty token, so fail
            // rather than fabricate a value.
            for block in response.blocks() {
                let index =
                    block
                        .block_index()
                        .ok_or_else(|| MetadataError::MalformedResponse {
                            snapshot_id: snapshot_id.to_string(),
                            field: "block_index".to_string(),
                        })?;
                let token =
                    block
                        .block_token()
                        .ok_or_else(|| MetadataError::MalformedResponse {
                            snapshot_id: snapshot_id.to_string(),
                            field: "block_token".to_string(),
                        })?;
                all_blocks.push(BlockInfo {
                    index,
                    token: token.to_string(),
                });
            }

            // Check for next page
            next_token = response.next_token().map(|s| s.to_string());
            if next_token.is_none() {
                break;
            }
        }

        Ok(all_blocks)
    }

    /// List the blocks that changed between two snapshots of the same volume.
    ///
    /// Used for incremental replication: `first` is the parent snapshot already
    /// replicated to the target, `second` is the newer source snapshot. Only the
    /// blocks returned here need to be copied; unchanged blocks are inherited from
    /// the parent at the destination.
    ///
    /// Each returned [`ChangedBlock`] is either a [`ChangedBlock::Copy`] (the
    /// block is readable from `second` via its `second_block_token`) or a
    /// [`ChangedBlock::Zero`] (the block was deleted/zeroed in `second`, so it
    /// has no second block token). The zeroed blocks must still be written as
    /// explicit zeros at the destination: because the destination is created on
    /// top of the parent snapshot, skipping them would leave the destination
    /// inheriting the parent's stale data for those indexes.
    pub async fn list_changed_blocks(
        &self,
        first_snapshot_id: &str,
        second_snapshot_id: &str,
    ) -> Result<Vec<ChangedBlock>, MetadataError> {
        let mut all_blocks = Vec::new();
        let mut next_token: Option<String> = None;

        loop {
            let mut request = self
                .source_ebs_client
                .list_changed_blocks()
                .first_snapshot_id(first_snapshot_id)
                .second_snapshot_id(second_snapshot_id)
                .max_results(LIST_BLOCKS_MAX_RESULTS);

            if let Some(token) = &next_token {
                request = request.next_token(token);
            }

            let response = request
                .send()
                .await
                .map_err(|e| MetadataError::ListFailed {
                    snapshot_id: second_snapshot_id.to_string(),
                    source: Box::new(e),
                })?;

            for block in response.changed_blocks() {
                let index =
                    block
                        .block_index()
                        .ok_or_else(|| MetadataError::MalformedResponse {
                            snapshot_id: second_snapshot_id.to_string(),
                            field: "block_index".to_string(),
                        })?;
                match block.second_block_token() {
                    // Present in the newer snapshot: copy it through.
                    Some(token) => all_blocks.push(ChangedBlock::Copy(BlockInfo {
                        index,
                        token: token.to_string(),
                    })),
                    // Absent in the newer snapshot: the block was zeroed/removed.
                    // It must be written as explicit zeros at the destination, or
                    // the destination would inherit the parent's stale data for
                    // this index.
                    None => all_blocks.push(ChangedBlock::Zero { index }),
                }
            }

            next_token = response.next_token().map(|s| s.to_string());
            if next_token.is_none() {
                break;
            }
        }

        Ok(all_blocks)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_config::BehaviorVersion;
    use aws_sdk_ec2::config::Region;

    fn create_mock_ec2_client() -> aws_sdk_ec2::Client {
        let config = aws_sdk_ec2::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new("us-east-1"))
            .build();
        aws_sdk_ec2::Client::from_conf(config)
    }

    fn create_mock_ebs_client() -> aws_sdk_ebs::Client {
        let config = aws_sdk_ebs::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new("us-east-1"))
            .build();
        aws_sdk_ebs::Client::from_conf(config)
    }

    #[tokio::test]
    async fn test_metadata_manager_construction() {
        // Verify MetadataManager builds with the (source EC2, source EBS) signature.
        let source_client = create_mock_ec2_client();
        let ebs_client = create_mock_ebs_client();

        let manager = MetadataManager::new(source_client, ebs_client);

        assert!(format!("{:?}", manager).contains("MetadataManager"));
    }
}
