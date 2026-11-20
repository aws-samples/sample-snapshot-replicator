// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

//! Block-level copying operations for EBS snapshots
//!
//! This module provides the `BlockCopier` for copying individual 512 KiB blocks
//! between source and destination snapshots using EBS Direct APIs.

use aws_sdk_ebs::primitives::ByteStream;
use aws_sdk_ebs::types::ChecksumAlgorithm;
use aws_sdk_ebs::Client as EbsClient;
use base64::{engine::general_purpose, Engine as _};
use sha2::{Digest, Sha256};
use thiserror::Error;

/// EBS Direct API block size, fixed at 512 KiB.
pub const EBS_BLOCK_SIZE: i32 = 524_288;

/// Manages block-level copy operations between source and destination snapshots
///
/// The BlockCopier handles reading blocks from a source snapshot and writing them
/// to a destination snapshot using the EBS Direct APIs (GetSnapshotBlock and
/// PutSnapshotBlock).
pub struct BlockCopier {
    /// EBS client for reading from source partition
    source_client: EbsClient,
    /// EBS client for writing to destination partition
    target_client: EbsClient,
}

/// Input parameters for a block copy operation
///
/// Contains the block index and token required to read a block from a snapshot
/// using the GetSnapshotBlock API.
#[derive(Clone, Debug)]
pub struct BlockInfo {
    /// Block index (0-based) within the snapshot
    pub index: i32,
    /// Block token from ListSnapshotBlocks API
    pub token: String,
}

/// A unit of work for the copy pipeline.
///
/// For a full copy every block is a [`ChangedBlock::Copy`]. For an incremental
/// copy, `ListChangedBlocks` can also report a block that existed in the parent
/// but is **absent** (zeroed/deallocated) in the newer source snapshot — such a
/// block has no second block token and cannot be read with `GetSnapshotBlock`.
/// Because the destination is created on top of the parent snapshot, simply
/// skipping that index would leave the destination inheriting the parent's stale
/// data. It must instead be written as an explicit zero block
/// ([`ChangedBlock::Zero`]) so the destination block is cleared.
#[derive(Clone, Debug)]
pub enum ChangedBlock {
    /// A block that is readable from the source and should be copied through.
    Copy(BlockInfo),
    /// A block that was zeroed/removed in the newer source snapshot and must be
    /// overwritten with zeros at the destination so it does not inherit the
    /// parent's data.
    Zero { index: i32 },
}

impl ChangedBlock {
    /// The block index this unit of work targets.
    pub fn index(&self) -> i32 {
        match self {
            ChangedBlock::Copy(info) => info.index,
            ChangedBlock::Zero { index } => *index,
        }
    }
}

/// Return value from a successful block copy operation
///
/// Contains the block index and SHA256 checksum for aggregate checksum calculation.
#[derive(Clone)]
pub struct BlockChecksum {
    /// Block index (0-based) within the snapshot
    pub index: i32,
    /// Base64-encoded SHA256 checksum of the block data
    pub checksum: String,
}

/// Errors that can occur during block copy operations
#[derive(Debug, Error)]
pub enum CopyError {
    /// Failed to read block from source snapshot
    #[error("Failed to get block {block_index} from snapshot {snapshot_id}")]
    GetBlockFailed {
        block_index: i32,
        snapshot_id: String,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    /// Failed to write block to destination snapshot
    #[error("Failed to put block {block_index} to snapshot {snapshot_id}")]
    PutBlockFailed {
        block_index: i32,
        snapshot_id: String,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    /// Source `GetSnapshotBlock` response carried no checksum
    #[error("Block {block_index} returned no checksum from the source snapshot")]
    MissingChecksum { block_index: i32 },
}

impl BlockCopier {
    /// Creates a new `BlockCopier` with the specified EBS clients.
    ///
    /// Transient failures (throttling, timeouts, transient network errors) are
    /// handled by the AWS SDK's adaptive retryer, configured when the clients are
    /// built in [`crate::auth::AuthManager`]. This type therefore performs a
    /// single `GetSnapshotBlock` → `PutSnapshotBlock` per call and surfaces the
    /// typed SDK error on failure.
    ///
    /// # Arguments
    ///
    /// * `source_client` - EBS client for reading from source partition
    /// * `target_client` - EBS client for writing to destination partition
    ///
    /// # Example
    ///
    /// ```no_run
    /// use snapshot_replicator::BlockCopier;
    /// use aws_sdk_ebs::Client as EbsClient;
    ///
    /// # async fn example(source: EbsClient, target: EbsClient) {
    /// let copier = BlockCopier::new(source, target);
    /// # }
    /// ```
    pub fn new(source_client: EbsClient, target_client: EbsClient) -> Self {
        Self {
            source_client,
            target_client,
        }
    }

    /// Process one [`ChangedBlock`]: copy it through from the source, or write an
    /// explicit zero block when the source no longer has it.
    ///
    /// A `ChangedBlock::Zero` arises only for incremental copies, when
    /// `ListChangedBlocks` reports a block that was present in the parent but
    /// zeroed/removed in the newer source snapshot. Writing zeros clears the
    /// block that the destination would otherwise inherit from its parent.
    ///
    /// # Errors
    ///
    /// * `CopyError::GetBlockFailed` - Failed to read a copied block from source
    /// * `CopyError::PutBlockFailed` - Failed to write the block to destination
    /// * `CopyError::MissingChecksum` - Source response carried no checksum
    pub async fn copy_changed_block(
        &self,
        source_snapshot_id: &str,
        dest_snapshot_id: &str,
        block: &ChangedBlock,
    ) -> Result<BlockChecksum, CopyError> {
        match block {
            ChangedBlock::Copy(info) => {
                self.copy_data_block(source_snapshot_id, dest_snapshot_id, info)
                    .await
            }
            ChangedBlock::Zero { index } => self.write_zero_block(dest_snapshot_id, *index).await,
        }
    }

    /// Write a 512 KiB all-zero block to the destination snapshot.
    ///
    /// Used to clear a block that was deallocated/zeroed in the newer source
    /// snapshot during an incremental copy. The SHA-256 checksum of the zero
    /// buffer is computed locally and passed to `PutSnapshotBlock` (which
    /// requires a checksum), and returned so the block participates in the
    /// LINEAR aggregate checksum like any other.
    ///
    /// # Errors
    ///
    /// * `CopyError::PutBlockFailed` - Failed to write the zero block
    pub async fn write_zero_block(
        &self,
        dest_snapshot_id: &str,
        index: i32,
    ) -> Result<BlockChecksum, CopyError> {
        let zero_block = vec![0u8; EBS_BLOCK_SIZE as usize];

        let mut hasher = Sha256::new();
        hasher.update(&zero_block);
        let checksum = general_purpose::STANDARD.encode(hasher.finalize());

        self.target_client
            .put_snapshot_block()
            .snapshot_id(dest_snapshot_id)
            .block_index(index)
            .block_data(ByteStream::from(zero_block))
            .checksum(&checksum)
            .checksum_algorithm(ChecksumAlgorithm::ChecksumAlgorithmSha256)
            .data_length(EBS_BLOCK_SIZE)
            .send()
            .await
            .map_err(|e| CopyError::PutBlockFailed {
                block_index: index,
                snapshot_id: dest_snapshot_id.to_string(),
                source: Box::new(e),
            })?;

        Ok(BlockChecksum { index, checksum })
    }

    /// Copies a single data block from source snapshot to destination snapshot
    ///
    /// Reads the block data from the source snapshot using `GetSnapshotBlock`,
    /// then writes it to the destination snapshot using `PutSnapshotBlock`.
    /// The checksum from the source is passed through to the destination for
    /// AWS to validate data integrity. Transient failures are retried by the
    /// SDK's adaptive retryer configured on the clients.
    ///
    /// # Arguments
    ///
    /// * `source_snapshot_id` - ID of the source snapshot to read from
    /// * `dest_snapshot_id` - ID of the destination snapshot to write to
    /// * `block` - Block information (index and token) for the block to copy
    ///
    /// # Returns
    ///
    /// * `Ok(BlockChecksum)` - Block checksum on success
    /// * `Err(CopyError)` - Error if the read or write operation fails
    ///
    /// # Errors
    ///
    /// * `CopyError::GetBlockFailed` - Failed to read block from source
    /// * `CopyError::PutBlockFailed` - Failed to write block to destination
    /// * `CopyError::MissingChecksum` - Source response carried no checksum
    ///
    /// # Example
    ///
    /// ```no_run
    /// use snapshot_replicator::{BlockCopier, BlockInfo};
    /// # use aws_sdk_ebs::Client as EbsClient;
    ///
    /// # async fn example(copier: BlockCopier) -> Result<(), Box<dyn std::error::Error>> {
    /// let block = BlockInfo {
    ///     index: 0,
    ///     token: "block-token-from-list-api".to_string(),
    /// };
    ///
    /// let checksum = copier.copy_data_block(
    ///     "snap-source123",
    ///     "snap-dest456",
    ///     &block
    /// ).await?;
    ///
    /// println!("Copied block {}", checksum.index);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn copy_data_block(
        &self,
        source_snapshot_id: &str,
        dest_snapshot_id: &str,
        block: &BlockInfo,
    ) -> Result<BlockChecksum, CopyError> {
        // Read block from source snapshot. The SDK's adaptive retryer handles
        // transient failures; the typed SDK error is preserved as the source so
        // the real cause (ThrottlingException, AccessDenied, …) is not lost.
        let get_response = self
            .source_client
            .get_snapshot_block()
            .snapshot_id(source_snapshot_id)
            .block_index(block.index)
            .block_token(&block.token)
            .send()
            .await
            .map_err(|e| CopyError::GetBlockFailed {
                block_index: block.index,
                snapshot_id: source_snapshot_id.to_string(),
                source: Box::new(e),
            })?;

        // Collect ByteStream into Bytes (zero-copy where possible). Passed
        // straight through to the destination's ByteStream::from.
        let block_data = get_response
            .block_data
            .collect()
            .await
            .map_err(|e| CopyError::GetBlockFailed {
                block_index: block.index,
                snapshot_id: source_snapshot_id.to_string(),
                source: Box::new(e),
            })?
            .into_bytes();

        // Extract checksum from response
        let checksum = get_response
            .checksum
            .ok_or_else(|| CopyError::MissingChecksum {
                block_index: block.index,
            })?;

        // Write block to destination snapshot
        self.target_client
            .put_snapshot_block()
            .snapshot_id(dest_snapshot_id)
            .block_index(block.index)
            .block_data(ByteStream::from(block_data))
            .checksum(&checksum)
            .checksum_algorithm(ChecksumAlgorithm::ChecksumAlgorithmSha256)
            .data_length(EBS_BLOCK_SIZE)
            .send()
            .await
            .map_err(|e| CopyError::PutBlockFailed {
                block_index: block.index,
                snapshot_id: dest_snapshot_id.to_string(),
                source: Box::new(e),
            })?;

        // Return block checksum for aggregate calculation
        Ok(BlockChecksum {
            index: block.index,
            checksum,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_block_info_creation() {
        let block_info = BlockInfo {
            index: 42,
            token: "test-token-123".to_string(),
        };
        assert_eq!(block_info.index, 42);
        assert_eq!(block_info.token, "test-token-123");
    }

    #[test]
    fn test_block_checksum_creation() {
        let checksum = BlockChecksum {
            index: 100,
            checksum: "abc123def456".to_string(),
        };
        assert_eq!(checksum.index, 100);
        assert_eq!(checksum.checksum, "abc123def456");
    }

    #[test]
    fn test_copy_error_get_block_failed() {
        let source_error = std::io::Error::new(std::io::ErrorKind::TimedOut, "Network timeout");
        let error = CopyError::GetBlockFailed {
            block_index: 123,
            snapshot_id: "snap-abc123".to_string(),
            source: Box::new(source_error),
        };
        let error_msg = format!("{}", error);
        assert!(error_msg.contains("Failed to get block 123 from snapshot snap-abc123"));
    }

    #[test]
    fn test_copy_error_put_block_failed() {
        let source_error =
            std::io::Error::new(std::io::ErrorKind::PermissionDenied, "Permission denied");
        let error = CopyError::PutBlockFailed {
            block_index: 456,
            snapshot_id: "snap-xyz789".to_string(),
            source: Box::new(source_error),
        };
        let error_msg = format!("{}", error);
        assert!(error_msg.contains("Failed to put block 456 to snapshot snap-xyz789"));
    }

    #[test]
    fn test_copy_error_debug() {
        let source_error = std::io::Error::other("test");
        let error = CopyError::GetBlockFailed {
            block_index: 1,
            snapshot_id: "snap-test".to_string(),
            source: Box::new(source_error),
        };
        let debug_str = format!("{:?}", error);
        assert!(debug_str.contains("GetBlockFailed"));
    }

    #[test]
    fn test_copy_error_structured_fields() {
        let source_error = std::io::Error::other("test error");
        let error = CopyError::GetBlockFailed {
            block_index: 999,
            snapshot_id: "snap-structured".to_string(),
            source: Box::new(source_error),
        };

        match error {
            CopyError::GetBlockFailed {
                block_index,
                snapshot_id,
                ..
            } => {
                assert_eq!(block_index, 999);
                assert_eq!(snapshot_id, "snap-structured");
            }
            _ => panic!("Wrong error variant"),
        }
    }

    #[test]
    fn test_copy_error_source_chain() {
        let source_error = std::io::Error::other("underlying error");
        let error = CopyError::PutBlockFailed {
            block_index: 42,
            snapshot_id: "snap-chain".to_string(),
            source: Box::new(source_error),
        };

        // Verify source error is accessible
        use std::error::Error;
        assert!(error.source().is_some());
    }

    #[test]
    fn test_changed_block_index_accessor() {
        let copy = ChangedBlock::Copy(BlockInfo {
            index: 7,
            token: "tok".to_string(),
        });
        let zero = ChangedBlock::Zero { index: 9 };
        assert_eq!(copy.index(), 7);
        assert_eq!(zero.index(), 9);
    }

    #[test]
    fn test_zero_block_checksum_is_sha256_of_512kib_of_zeros() {
        // The zero-block write path hashes a 512 KiB all-zero buffer. This
        // reproduces that computation independently and checks the base64
        // SHA-256 matches, so the value sent to PutSnapshotBlock is correct.
        let zero_block = vec![0u8; EBS_BLOCK_SIZE as usize];
        let mut hasher = Sha256::new();
        hasher.update(&zero_block);
        let expected = general_purpose::STANDARD.encode(hasher.finalize());

        // Decodes to exactly 32 bytes (a SHA-256 digest).
        let raw = general_purpose::STANDARD.decode(&expected).unwrap();
        assert_eq!(raw.len(), 32);

        // Known SHA-256 of 524288 zero bytes (stable test vector).
        assert_eq!(expected, "B4VNL+8pega6gWheZgwzLeNtXRjVRpJ9MNqtbX/aFUE=");
    }
}
