// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

//! Snapshot creation and management
//!
//! This module handles creating destination snapshots using the EBS Direct APIs,
//! including client token generation for idempotent operations.

use crate::block_copier::{BlockChecksum, BlockCopier, ChangedBlock, CopyError};
use crate::checksum::aggregate_checksum;
use crate::config::ReplicationConfig;
use crate::metadata::{MetadataError, MetadataManager};
use crate::progress::ProgressReporter;
use crate::state::{ResumedState, StateFile};
use futures::stream::{self, StreamExt, TryStreamExt};
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;

/// Errors that can occur during snapshot operations
#[derive(Debug, Error)]
pub enum SnapshotError {
    #[error("Failed to complete snapshot {snapshot_id}")]
    CompletionFailed {
        snapshot_id: String,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[error("Timeout waiting for snapshot {snapshot_id} after {elapsed_secs}s")]
    WaitTimeout {
        snapshot_id: String,
        elapsed_secs: u64,
    },

    #[error("AWS API error: {message}")]
    ApiError { message: String },

    #[error("Failed to describe snapshot {snapshot_id}")]
    DescribeFailed {
        snapshot_id: String,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[error("Failed to get source snapshot metadata for {snapshot_id}")]
    SourceMetadata {
        snapshot_id: String,
        #[source]
        source: MetadataError,
    },

    #[error("Access denied to KMS key `{key_arn}` — verify the target profile has kms:CreateGrant and kms:Decrypt permissions")]
    KmsAccessDenied {
        key_arn: String,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[error("Failed to start destination snapshot in {target_region}")]
    StartSnapshotFailed {
        target_region: String,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[error(
        "The destination snapshot {dest_snapshot_id} from a previous run is in the `error` state. \
         EBS cancels a pending snapshot if no block is written for ~60 minutes, so this usually \
         means the earlier run stalled (for example retrying unreadable source blocks for over an \
         hour). Delete the errored snapshot and its state file \
         (snapshot-replicator-state-{dest_snapshot_id}.csv), then re-run to start a fresh copy."
    )]
    PreviousAttemptCancelled { dest_snapshot_id: String },

    #[error("Block copy error for snapshot {snapshot_id}")]
    BlockCopyError {
        snapshot_id: String,
        #[source]
        source: CopyError,
    },
}

impl From<CopyError> for SnapshotError {
    fn from(err: CopyError) -> Self {
        let snapshot_id = match &err {
            CopyError::GetBlockFailed { snapshot_id, .. } => snapshot_id.clone(),
            CopyError::PutBlockFailed { snapshot_id, .. } => snapshot_id.clone(),
            CopyError::MissingChecksum { .. } => "unknown".to_string(),
        };
        SnapshotError::BlockCopyError {
            snapshot_id,
            source: err,
        }
    }
}

/// Status of a snapshot returned by the StartSnapshot API
#[derive(Debug, Clone, PartialEq)]
pub enum SnapshotStatus {
    Pending,
    Completed,
    Error,
}

impl From<Option<&aws_sdk_ebs::types::Status>> for SnapshotStatus {
    fn from(status: Option<&aws_sdk_ebs::types::Status>) -> Self {
        match status {
            Some(aws_sdk_ebs::types::Status::Completed) => SnapshotStatus::Completed,
            Some(aws_sdk_ebs::types::Status::Error) => SnapshotStatus::Error,
            _ => SnapshotStatus::Pending,
        }
    }
}

/// Result of a StartSnapshot API call
#[derive(Debug)]
pub struct StartSnapshotResult {
    pub snapshot_id: String,
    pub status: SnapshotStatus,
}

/// Parameters describing the destination snapshot to create.
///
/// Groups the destination-shaping options so the create call stays readable as
/// features (encryption, incremental parents) are added.
#[derive(Debug, Default, Clone)]
pub struct DestinationSpec {
    /// Volume size in GiB (must match the source snapshot).
    pub volume_size_gib: i64,
    /// Optional description to set on the destination snapshot.
    pub description: Option<String>,
    /// Explicit encryption request (`Some(true)` from `--encrypt` /
    /// `--kms-key-id`). `None` omits `Encrypted` from `StartSnapshot`, so the
    /// target account's encryption-by-default setting decides the outcome.
    pub encrypted: Option<bool>,
    /// Optional target KMS key ARN. `None` omits `KmsKeyArn`, so EBS uses the
    /// target account's default key when the snapshot is encrypted.
    pub kms_key_id: Option<String>,
    /// Optional destination parent snapshot ID for incremental copy.
    pub parent_dest_snapshot_id: Option<String>,
    /// Tags to apply to the destination snapshot at creation time.
    ///
    /// Applied directly by `StartSnapshot` so provenance (for example
    /// `SourceSnapshotArn`) is present from the first moment the snapshot
    /// exists, rather than in a separate post-completion `CreateTags` call that
    /// could fail and leave the replica untagged.
    pub tags: Vec<aws_sdk_ebs::types::Tag>,
}

/// Inputs for a single [`SnapshotManager::replicate_snapshot`] call.
///
/// Groups the per-run parameters so the entry point stays readable and avoids a
/// long positional argument list.
#[derive(Debug, Default, Clone)]
pub struct ReplicationRequest {
    /// Source snapshot ID.
    pub source_snapshot_id: String,
    /// Source region (used to build the partition-correct `SourceSnapshotArn`).
    pub source_region: String,
    /// Target region for the replicated snapshot.
    pub target_region: String,
    /// Blocks to copy. For a full copy this is every data block in the source;
    /// for an incremental copy it is only the changed blocks.
    pub blocks: Vec<ChangedBlock>,
    /// Explicit encryption request (`None` = leave it to the target account's
    /// defaults; the source snapshot's encryption state is not consulted).
    pub encrypted: Option<bool>,
    /// Target KMS key ARN (`None` = the target account's default key when
    /// the destination is encrypted).
    pub kms_key_id: Option<String>,
    /// Destination parent snapshot for incremental copy (`None` = full copy).
    pub parent_dest_snapshot_id: Option<String>,
}

/// Encryption state of a completed destination snapshot, as reported by
/// `DescribeSnapshots` on the target account.
///
/// `CompleteSnapshot` itself returns only a status, so this is read from the
/// authoritative `DescribeSnapshots` response once the snapshot is `completed`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DestinationEncryption {
    /// `Some(true)` / `Some(false)` as reported by EC2; `None` if the field was
    /// absent from the response.
    pub encrypted: Option<bool>,
    /// KMS key the destination is encrypted with, when reported.
    pub kms_key_id: Option<String>,
}

/// Outcome of a [`SnapshotManager::replicate_snapshot`] call.
///
/// Distinguishes a run that actually copied blocks from one that found the
/// destination already complete (via the idempotent token or an authoritative
/// `DescribeSnapshots` check) and copied nothing, so the caller can report an
/// accurate summary instead of claiming a copy that did not happen.
#[derive(Debug, Clone)]
pub struct ReplicationOutcome {
    /// Destination snapshot ID.
    pub dest_snapshot_id: String,
    /// `true` when the destination was already complete and this run copied no
    /// blocks (idempotent reuse); `false` when blocks were copied this run.
    pub reused_existing: bool,
    /// Encryption state of the destination, read after completion. `None` when
    /// this run did not observe it (the reused-existing paths).
    pub encryption: Option<DestinationEncryption>,
}

/// Manager for snapshot operations
#[derive(Debug)]
pub struct SnapshotManager {
    /// EBS client for source partition
    source_ebs_client: aws_sdk_ebs::Client,
    /// EBS client for target partition
    target_ebs_client: aws_sdk_ebs::Client,
    /// EC2 client for target partition
    target_ec2_client: aws_sdk_ec2::Client,
    /// Metadata manager for snapshot metadata operations
    metadata_manager: Arc<MetadataManager>,
    /// Configuration for concurrency and retry settings
    config: ReplicationConfig,
}

impl SnapshotManager {
    /// Create a new SnapshotManager
    pub fn new(
        source_ebs_client: aws_sdk_ebs::Client,
        target_ebs_client: aws_sdk_ebs::Client,
        target_ec2_client: aws_sdk_ec2::Client,
        metadata_manager: Arc<MetadataManager>,
        config: ReplicationConfig,
    ) -> Self {
        Self {
            source_ebs_client,
            target_ebs_client,
            target_ec2_client,
            metadata_manager,
            config,
        }
    }

    /// Generate a deterministic client token for idempotent snapshot creation.
    ///
    /// The token is `sha256("v2:" + source_id:target_region:encrypted:kms:parent)`.
    /// It encodes every input that changes the resulting snapshot, so re-running
    /// the same command replays the same destination (idempotent) while changing
    /// any input produces a new, independent copy.
    ///
    /// The `encrypted` request is included because `--encrypt` (default EBS key)
    /// and a plain run both leave `kms_key_id = None`; without it in the token,
    /// toggling `--encrypt` would reuse the token with a different encryption
    /// request and EBS would reject it with `ConflictException`. `None` (no
    /// flag; the target's defaults apply) hashes as `default`. The
    /// `v2:` prefix namespaces this format: a pending snapshot started by an
    /// older token will not be found, so its resume restarts (acceptable for a
    /// sample; the data copy is idempotent).
    pub fn generate_client_token(
        &self,
        source_snapshot_id: &str,
        target_region: &str,
        encrypted: Option<bool>,
        kms_key_id: &Option<String>,
        parent_dest_snapshot_id: &Option<String>,
    ) -> String {
        use sha2::{Digest, Sha256};

        let encrypted_part = match encrypted {
            Some(true) => "true",
            Some(false) => "false",
            None => "default",
        };
        let kms_part = kms_key_id.as_deref().unwrap_or("none");
        let parent_part = parent_dest_snapshot_id.as_deref().unwrap_or("none");
        let input = format!(
            "v2:{}:{}:{}:{}:{}",
            source_snapshot_id, target_region, encrypted_part, kms_part, parent_part
        );

        let mut hasher = Sha256::new();
        hasher.update(input.as_bytes());
        let result = hasher.finalize();

        // Lowercase hex of the 32-byte digest (64 characters). Encoded by hand
        // because sha2 0.11 digests no longer implement `LowerHex`.
        result.iter().map(|b| format!("{b:02x}")).collect()
    }

    /// Create a destination snapshot using the EBS StartSnapshot API
    ///
    /// Uses a client token for idempotency — calling with the same inputs returns
    /// the existing pending snapshot rather than creating a new one.
    ///
    /// When `spec.parent_dest_snapshot_id` is set, the new snapshot is created as
    /// an incremental delta on top of that parent (only changed blocks are written).
    pub async fn create_destination_snapshot(
        &self,
        source_snapshot_id: &str,
        target_region: &str,
        spec: DestinationSpec,
    ) -> Result<StartSnapshotResult, SnapshotError> {
        let client_token = self.generate_client_token(
            source_snapshot_id,
            target_region,
            spec.encrypted,
            &spec.kms_key_id,
            &spec.parent_dest_snapshot_id,
        );

        let mut request = self
            .target_ebs_client
            .start_snapshot()
            .volume_size(spec.volume_size_gib)
            .client_token(&client_token)
            .set_encrypted(start_snapshot_encrypted_param(
                spec.parent_dest_snapshot_id.is_some(),
                spec.encrypted,
            ));

        if let Some(desc) = spec.description {
            request = request.description(desc);
        }

        if let Some(key_id) = &spec.kms_key_id {
            request = request.kms_key_arn(key_id);
        }

        if let Some(parent_id) = &spec.parent_dest_snapshot_id {
            request = request.parent_snapshot_id(parent_id);
        }

        // Apply tags at creation so provenance (e.g. SourceSnapshotArn) exists
        // from the first moment the snapshot does, independent of any later call.
        if !spec.tags.is_empty() {
            request = request.set_tags(Some(spec.tags.clone()));
        }

        let response = request.send().await.map_err(|e| {
            use aws_sdk_ebs::error::ProvideErrorMetadata;

            // Classify by the service error *code* (and message), not by matching
            // on Debug output. Only a genuine KMS/key-access denial maps to the
            // KMS-specific remediation; a plain IAM/endpoint denial keeps the
            // typed SDK error as `#[source]` so the real cause is visible.
            let code = e.code().unwrap_or_default();
            let message = e.message().unwrap_or_default();

            if start_snapshot_denial_is_kms(code, message, spec.kms_key_id.is_some()) {
                return SnapshotError::KmsAccessDenied {
                    key_arn: spec.kms_key_id.clone().unwrap_or_default(),
                    source: Box::new(e),
                };
            }

            SnapshotError::StartSnapshotFailed {
                target_region: target_region.to_string(),
                source: Box::new(e),
            }
        })?;

        let snapshot_id = response
            .snapshot_id()
            .ok_or_else(|| SnapshotError::StartSnapshotFailed {
                target_region: target_region.to_string(),
                source: Box::new(std::io::Error::other(
                    "StartSnapshot response did not include a snapshot ID",
                )),
            })?
            .to_string();

        let status = SnapshotStatus::from(response.status());

        Ok(StartSnapshotResult {
            snapshot_id,
            status,
        })
    }

    /// Copies blocks concurrently in a single pass, stopping as soon as a block
    /// fails.
    ///
    /// Retry ownership is deliberately simple and explicit for a sample:
    /// - **Transient** failures (throttling, timeouts, transient network errors)
    ///   are retried *inside* each `GetSnapshotBlock` / `PutSnapshotBlock` call by
    ///   the AWS SDK's adaptive retryer (configured in [`crate::auth`], bounded by
    ///   `--max-attempts`). There is no second retry loop on top of it.
    /// - A failure that survives the SDK's retries is treated as **fatal**: it is
    ///   almost always systemic (IAM/KMS denial, a sealed/`completed` destination,
    ///   a bad snapshot), not something a wholesale re-attempt would fix. So this
    ///   stops feeding new work on the first such error.
    ///
    /// Shutdown semantics: on the first error we stop pulling new blocks into the
    /// pipeline and drop the requests still in flight, then return that first
    /// error. Every block that already completed successfully was recorded to the
    /// state file, so re-running the same command resumes the remainder.
    ///
    /// # Arguments
    /// * `source_snapshot_id` - Source snapshot ID
    /// * `dest_snapshot_id` - Destination snapshot ID
    /// * `blocks` - Blocks to copy
    /// * `total_blocks` - Total block count (includes already-resumed blocks, for progress display)
    /// * `initial_position` - Number of already-completed blocks (for resume progress display)
    /// * `state_file` - Optional state file for recording completions (resume support)
    ///
    /// # Returns
    /// Vector of block checksums sorted by index on full success, or the first
    /// block failure encountered.
    pub async fn copy_blocks_concurrently(
        &self,
        source_snapshot_id: &str,
        dest_snapshot_id: &str,
        blocks: Vec<ChangedBlock>,
        total_blocks: usize,
        initial_position: usize,
        state_file: Option<Arc<StateFile>>,
    ) -> Result<Vec<BlockChecksum>, CopyError> {
        if blocks.is_empty() {
            return Ok(Vec::new());
        }

        let progress = Arc::new(ProgressReporter::new(total_blocks, initial_position));
        let copier = Arc::new(BlockCopier::new(
            self.source_ebs_client.clone(),
            self.target_ebs_client.clone(),
        ));

        let source_id = source_snapshot_id.to_string();
        let dest_id = dest_snapshot_id.to_string();

        // `try_collect` over a `buffer_unordered` stream fails fast: on the
        // first error it stops polling, so no new blocks are pulled into the
        // pipeline and the remaining source reads/writes are never issued.
        // In-flight requests at that moment are dropped. Every block that
        // already succeeded was recorded to the state file inside the closure,
        // so re-running the same command resumes the remainder.
        let mut checksums: Vec<BlockChecksum> = stream::iter(blocks)
            .map(|block| {
                let copier = copier.clone();
                let progress = progress.clone();
                let state_file = state_file.clone();
                let source_id = source_id.clone();
                let dest_id = dest_id.clone();

                async move {
                    let checksum = copier
                        .copy_changed_block(&source_id, &dest_id, &block)
                        .await?;
                    progress.update();
                    if let Some(ref sf) = state_file {
                        sf.record(checksum.clone());
                    }
                    Ok(checksum)
                }
            })
            .buffer_unordered(self.config.concurrency)
            .try_collect()
            .await?;

        progress.finish();
        checksums.sort_by_key(|c| c.index);
        Ok(checksums)
    }

    /// Complete destination snapshot with aggregate checksum
    pub async fn complete_snapshot(
        &self,
        dest_snapshot_id: &str,
        block_checksums: Vec<BlockChecksum>,
    ) -> Result<(), SnapshotError> {
        let aggregate_checksum =
            aggregate_checksum(&block_checksums).map_err(|e| SnapshotError::CompletionFailed {
                snapshot_id: dest_snapshot_id.to_string(),
                source: Box::new(e),
            })?;
        let changed_blocks_count = block_checksums.len() as i32;

        self.target_ebs_client
            .complete_snapshot()
            .snapshot_id(dest_snapshot_id)
            .changed_blocks_count(changed_blocks_count)
            .checksum(&aggregate_checksum)
            .checksum_algorithm(aws_sdk_ebs::types::ChecksumAlgorithm::ChecksumAlgorithmSha256)
            .checksum_aggregation_method(
                aws_sdk_ebs::types::ChecksumAggregationMethod::ChecksumAggregationLinear,
            )
            .send()
            .await
            .map_err(|e| SnapshotError::CompletionFailed {
                snapshot_id: dest_snapshot_id.to_string(),
                source: Box::new(e),
            })?;

        Ok(())
    }

    /// Query the authoritative server-side state of the destination snapshot via
    /// `DescribeSnapshots` on the target EC2 client.
    ///
    /// `StartSnapshot` replays the *original* result for an idempotent client
    /// token, so for a short window after a previous run completed it can still
    /// report the snapshot as `pending`. `DescribeSnapshots` returns the current
    /// truth, so this is used both before copying (to skip an
    /// already-`completed` destination) and after a copy failure (to recognise
    /// that the destination is in fact done — a `PutSnapshotBlock` against a
    /// sealed snapshot fails with `ValidationException: Snapshot is already
    /// completed`).
    ///
    /// Returns `Ok(None)` when the snapshot is not yet visible (eventual
    /// consistency) or has no state field.
    async fn destination_state(
        &self,
        dest_snapshot_id: &str,
    ) -> Result<Option<SnapshotStatus>, SnapshotError> {
        let response = self
            .target_ec2_client
            .describe_snapshots()
            .snapshot_ids(dest_snapshot_id)
            .send()
            .await
            .map_err(|e| SnapshotError::DescribeFailed {
                snapshot_id: dest_snapshot_id.to_string(),
                source: Box::new(e),
            })?;

        let state = response
            .snapshots()
            .first()
            .and_then(|s| s.state())
            .map(|s| match s {
                aws_sdk_ec2::types::SnapshotState::Completed => SnapshotStatus::Completed,
                aws_sdk_ec2::types::SnapshotState::Error => SnapshotStatus::Error,
                _ => SnapshotStatus::Pending,
            });

        Ok(state)
    }

    /// Wait for destination snapshot to reach completed state.
    ///
    /// Returns the destination's encryption state as reported by the same
    /// `DescribeSnapshots` response that showed it `completed`.
    pub async fn wait_for_completion(
        &self,
        dest_snapshot_id: &str,
    ) -> Result<DestinationEncryption, SnapshotError> {
        // Poll until the destination snapshot is `completed`, using exponential
        // backoff so a snapshot that finalises quickly (the common case) is
        // detected in a couple of seconds instead of waiting a fixed interval.
        // The interval grows from 2s to a 15s cap, bounded by a total budget so
        // the overall timeout is unchanged. For long-running or cross-partition
        // cases the caller can verify the final state out of band.
        const MAX_WAIT_SECS: u64 = 300;
        const INITIAL_INTERVAL_SECS: u64 = 2;
        const MAX_INTERVAL_SECS: u64 = 15;

        let start = std::time::Instant::now();
        let mut interval = INITIAL_INTERVAL_SECS;

        loop {
            let response = self
                .target_ec2_client
                .describe_snapshots()
                .snapshot_ids(dest_snapshot_id)
                .send()
                .await
                .map_err(|e| SnapshotError::DescribeFailed {
                    snapshot_id: dest_snapshot_id.to_string(),
                    source: Box::new(e),
                })?;

            let snapshot = response.snapshots().first();
            match snapshot.and_then(|s| s.state()) {
                // Done.
                Some(aws_sdk_ec2::types::SnapshotState::Completed) => {
                    return Ok(DestinationEncryption {
                        encrypted: snapshot.and_then(|s| s.encrypted()),
                        kms_key_id: snapshot.and_then(|s| s.kms_key_id()).map(str::to_string),
                    });
                }
                // EBS reports the snapshot failed server-side.
                Some(aws_sdk_ec2::types::SnapshotState::Error) => {
                    return Err(SnapshotError::CompletionFailed {
                        snapshot_id: dest_snapshot_id.to_string(),
                        source: "snapshot entered the Error state".into(),
                    });
                }
                // Pending, recoverable, or not yet visible (eventual
                // consistency): keep waiting.
                _ => {}
            }

            if start.elapsed().as_secs() >= MAX_WAIT_SECS {
                return Err(SnapshotError::WaitTimeout {
                    snapshot_id: dest_snapshot_id.to_string(),
                    elapsed_secs: MAX_WAIT_SECS,
                });
            }

            tokio::time::sleep(Duration::from_secs(interval)).await;
            interval = (interval * 2).min(MAX_INTERVAL_SECS);
        }
    }

    /// Replicate snapshot from source to destination
    ///
    /// Supports automatic resume: if a pending destination snapshot already exists
    /// (via idempotent client token), loads the state file and skips already-copied blocks.
    ///
    /// # Arguments
    /// * `request` - The [`ReplicationRequest`] describing the source, target,
    ///   blocks, and encryption/incremental options for this run.
    ///
    /// # Returns
    /// * `Ok(ReplicationOutcome)` - Destination snapshot ID plus whether an
    ///   already-complete destination was reused (no blocks copied this run)
    /// * `Err(SnapshotError)` - Error if replication fails
    pub async fn replicate_snapshot(
        &self,
        request: ReplicationRequest,
    ) -> Result<ReplicationOutcome, SnapshotError> {
        let ReplicationRequest {
            source_snapshot_id,
            source_region,
            target_region,
            blocks,
            encrypted,
            kms_key_id,
            parent_dest_snapshot_id,
        } = request;
        let source_snapshot_id = source_snapshot_id.as_str();
        let source_region = source_region.as_str();
        let target_region = target_region.as_str();

        // Get source snapshot metadata
        let source_metadata = self
            .metadata_manager
            .get_snapshot_metadata(source_snapshot_id)
            .await
            .map_err(|e: MetadataError| SnapshotError::SourceMetadata {
                snapshot_id: source_snapshot_id.to_string(),
                source: e,
            })?;

        // Encryption is only requested explicitly (--encrypt / --kms-key-id).
        // Otherwise `Encrypted` and `KmsKeyArn` are omitted and the target
        // account's encryption-by-default setting decides. The source
        // snapshot's own encryption state is deliberately not consulted.

        // Build the provenance + source tags once, so they are applied by
        // StartSnapshot itself. The replica is therefore tagged from the moment
        // it exists, with no separate post-completion CreateTags call that could
        // fail and leave it untagged.
        let tags = build_destination_tags(&source_metadata, source_region, source_snapshot_id);

        // Create (or retrieve existing) destination snapshot
        let StartSnapshotResult {
            snapshot_id: dest_snapshot_id,
            status,
        } = self
            .create_destination_snapshot(
                source_snapshot_id,
                target_region,
                DestinationSpec {
                    volume_size_gib: source_metadata.volume_size,
                    description: source_metadata.description.clone(),
                    encrypted,
                    kms_key_id,
                    parent_dest_snapshot_id,
                    tags,
                },
            )
            .await?;

        match status {
            SnapshotStatus::Completed => {
                // The idempotent client token replays the original result once
                // the destination has completed: re-running the same command
                // returns this existing snapshot rather than creating a new one.
                // Since the provenance tags are now applied at StartSnapshot,
                // the replica is already tagged, so there is nothing to
                // reconcile here — just report done.
                log::info!(
                    "Destination snapshot {} already completed; nothing to copy",
                    dest_snapshot_id
                );
                return Ok(ReplicationOutcome {
                    dest_snapshot_id,
                    reused_existing: true,
                    encryption: None,
                });
            }
            SnapshotStatus::Error => {
                // EBS auto-cancels a pending snapshot if no block is written for
                // ~60 minutes. Reaching this state via the idempotent client
                // token means a previous run left the destination in `error`,
                // typically because it stalled (e.g. retrying unreadable source
                // blocks) for over an hour. Surface an actionable message rather
                // than a generic failure.
                return Err(SnapshotError::PreviousAttemptCancelled {
                    dest_snapshot_id: dest_snapshot_id.clone(),
                });
            }
            SnapshotStatus::Pending => {
                // `StartSnapshot` replays the *original* result for an idempotent
                // token, so just after a previous run completed it can still
                // report `pending` for a short window. In that case every
                // PutSnapshotBlock would then fail with `ValidationException:
                // Snapshot is already completed`.
                // Consult the authoritative server state before copying so we
                // skip (or reject) a destination that is really done or errored.
                match self.destination_state(&dest_snapshot_id).await? {
                    Some(SnapshotStatus::Completed) => {
                        log::info!(
                            "Destination snapshot {} is already completed (per DescribeSnapshots); nothing to copy",
                            dest_snapshot_id
                        );
                        let _ = StateFile::cleanup(&dest_snapshot_id);
                        return Ok(ReplicationOutcome {
                            dest_snapshot_id,
                            reused_existing: true,
                            encryption: None,
                        });
                    }
                    Some(SnapshotStatus::Error) => {
                        return Err(SnapshotError::PreviousAttemptCancelled {
                            dest_snapshot_id: dest_snapshot_id.clone(),
                        });
                    }
                    // Pending or not yet visible: proceed to copy as normal.
                    _ => {}
                }
            }
        }

        // Load any existing state for resume
        let resumed_state =
            StateFile::load(&dest_snapshot_id).map_err(|e| SnapshotError::ApiError {
                message: format!("Failed to load state file: {}", e),
            })?;

        let (blocks_to_copy, resumed_checksums) = match resumed_state {
            Some(state) => {
                let remaining = filter_remaining_blocks(blocks, &state);
                log::info!(
                    "Resuming: {}/{} blocks already copied, {} remaining",
                    state.completed_indexes.len(),
                    state.completed_indexes.len() + remaining.len(),
                    remaining.len()
                );
                (remaining, state.checksums)
            }
            None => {
                log::info!("Starting replication of {}", source_snapshot_id);
                (blocks, Vec::new())
            }
        };

        let total_blocks = blocks_to_copy.len() + resumed_checksums.len();
        let initial_position = resumed_checksums.len();

        // Create state file for recording new completions
        let state_file =
            Arc::new(
                StateFile::new(&dest_snapshot_id).map_err(|e| SnapshotError::ApiError {
                    message: format!("Failed to create state file: {}", e),
                })?,
            );

        // Copy remaining blocks
        let copy_result = self
            .copy_blocks_concurrently(
                source_snapshot_id,
                &dest_snapshot_id,
                blocks_to_copy,
                total_blocks,
                initial_position,
                Some(state_file.clone()),
            )
            .await;

        // Flush state file (on both success and failure paths) before acting on
        // the result, so queued records are not lost.
        Arc::try_unwrap(state_file)
            .map_err(|_| SnapshotError::ApiError {
                message: "State file still referenced after copy".to_string(),
            })?
            .close()
            .await;

        let new_checksums = match copy_result {
            Ok(checksums) => checksums,
            Err(copy_err) => {
                // A copy failure can mean the destination was *already completed*
                // server-side while `StartSnapshot` still reported it pending
                // (eventual consistency): every `PutSnapshotBlock` then fails
                // with `ValidationException: Snapshot is already completed`.
                // Consult the authoritative state before surfacing the error — if
                // the snapshot is in fact done, the replica is complete and this
                // run has nothing left to do.
                if let Some(SnapshotStatus::Completed) =
                    self.destination_state(&dest_snapshot_id).await?
                {
                    log::info!(
                        "Destination snapshot {} is already completed (per DescribeSnapshots); \
                         treating the copy failure as a no-op",
                        dest_snapshot_id
                    );
                    let _ = StateFile::cleanup(&dest_snapshot_id);
                    return Ok(ReplicationOutcome {
                        dest_snapshot_id,
                        reused_existing: true,
                        encryption: None,
                    });
                }
                return Err(copy_err.into());
            }
        };

        // Merge resumed + new checksums, sorted by index
        let all_checksums = merge_checksums(resumed_checksums, new_checksums);

        // Complete snapshot
        self.complete_snapshot(&dest_snapshot_id, all_checksums)
            .await?;

        // Wait for completion
        let encryption = self.wait_for_completion(&dest_snapshot_id).await?;

        // Tags/description were applied by StartSnapshot, so there is no
        // post-completion metadata copy to do — just remove the local resume
        // state file now that the destination is complete.
        if let Err(e) = StateFile::cleanup(&dest_snapshot_id) {
            log::warn!("Failed to clean up state file: {}", e);
        }

        Ok(ReplicationOutcome {
            dest_snapshot_id,
            reused_existing: false,
            encryption: Some(encryption),
        })
    }
}

/// Value for `StartSnapshot`'s `Encrypted` parameter.
///
/// EBS rejects any request that carries both `Encrypted` and `ParentSnapshotId`
/// (even `Encrypted: false`) with `ValidationException: ParentSnapshotId and
/// Encrypt cannot be specified together`. An incremental snapshot inherits its
/// encryption from the parent, so the parameter must be omitted whenever a
/// parent is given. Without a parent, `None` (no explicit request) is passed
/// through so the target account's encryption-by-default setting applies.
fn start_snapshot_encrypted_param(has_parent: bool, encrypted: Option<bool>) -> Option<bool> {
    if has_parent {
        None
    } else {
        encrypted
    }
}

/// Decide whether a failed `StartSnapshot` denial should be reported as a
/// KMS-specific error rather than a generic one.
///
/// Classifies by the service error **code** and message (not Debug output). A
/// denial is treated as KMS-related when the error is an access denial **and**
/// either the code/message names KMS or a key, or the caller explicitly supplied
/// a target KMS key (so an access denial is almost certainly about that key). A
/// plain IAM/endpoint/SCP denial returns `false`, so the typed SDK error is kept
/// as the source instead of showing misleading KMS remediation.
///
/// Pure helper so the classification rule is unit-testable without constructing
/// a live `SdkError`.
fn start_snapshot_denial_is_kms(code: &str, message: &str, has_explicit_kms_key: bool) -> bool {
    // EBS surfaces a KMS permission denial as a top-level `ValidationException`
    // ("Invalid parameters for a dependent service: …AWSKMSException… not
    // authorized to perform: kms:GenerateDataKey … AccessDeniedException"), so the
    // access-denied and KMS signals can live in the *message* even when the
    // top-level `code` is not `AccessDenied`. Check both the code and the message
    // for each signal.
    let code_l = code.to_ascii_lowercase();
    let msg_l = message.to_ascii_lowercase();

    let access_denied = code_l.contains("accessdenied") || msg_l.contains("accessdenied");
    if !access_denied {
        return false;
    }

    let mentions_kms = code_l.contains("kms") || msg_l.contains("kms") || msg_l.contains("key");
    mentions_kms || has_explicit_kms_key
}

/// Build the tags to apply to the destination snapshot at creation time.
///
/// These are passed to `StartSnapshot` so the replica is tagged from the moment
/// it exists. The set is:
/// - every user tag from the source snapshot, **except** AWS-reserved `aws:*`
///   tags (which the API rejects on create/tag calls); and
/// - a `SourceSnapshotArn` provenance tag built with the partition that matches
///   the *source* region (`aws`, `aws-us-gov`, `aws-cn`, or `aws-eusc`).
///
/// The source description is set as the snapshot's `Description` field by the
/// caller, so it is deliberately **not** duplicated here as a tag.
///
/// Pure helper (no AWS client) so the filtering/provenance rules are
/// unit-testable.
fn build_destination_tags(
    source_metadata: &crate::metadata::SnapshotMetadata,
    source_region: &str,
    source_snapshot_id: &str,
) -> Vec<aws_sdk_ebs::types::Tag> {
    let partition = crate::region::partition_for_region(source_region);
    let source_arn = format!("arn:{partition}:ec2:{source_region}::snapshot/{source_snapshot_id}");

    let mut tags: Vec<aws_sdk_ebs::types::Tag> = source_metadata
        .tags
        .iter()
        .filter_map(|t| {
            let key = t.key()?;
            // AWS-reserved tags cannot be set by callers; copying them through
            // would make StartSnapshot reject the whole request.
            if key.starts_with("aws:") {
                return None;
            }
            Some(
                aws_sdk_ebs::types::Tag::builder()
                    .key(key)
                    .value(t.value().unwrap_or_default())
                    .build(),
            )
        })
        .collect();

    tags.push(
        aws_sdk_ebs::types::Tag::builder()
            .key("SourceSnapshotArn")
            .value(source_arn)
            .build(),
    );

    tags
}

/// Drop the blocks already recorded as completed in a resume state, returning
/// only those still to copy.
///
/// Pure helper extracted from `replicate_snapshot` so the resume-filtering rule
/// is unit-testable without an AWS client.
fn filter_remaining_blocks(blocks: Vec<ChangedBlock>, state: &ResumedState) -> Vec<ChangedBlock> {
    blocks
        .into_iter()
        .filter(|b| !state.completed_indexes.contains(&b.index()))
        .collect()
}

/// Merge resumed checksums with newly copied ones, sorted by block index.
///
/// Pure helper extracted from `replicate_snapshot` so the merge/sort rule is
/// unit-testable without an AWS client. The sorted order matters because the
/// LINEAR aggregate checksum depends on index order.
fn merge_checksums(resumed: Vec<BlockChecksum>, new: Vec<BlockChecksum>) -> Vec<BlockChecksum> {
    let mut all = resumed;
    all.extend(new);
    all.sort_by_key(|c| c.index);
    all
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_sdk_ebs::config::{BehaviorVersion, Region};
    use aws_sdk_ebs::Config as EbsConfig;
    use aws_sdk_ec2::config::Config as Ec2Config;

    fn create_mock_ebs_client() -> aws_sdk_ebs::Client {
        let config = EbsConfig::builder()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new("us-east-1"))
            .build();
        aws_sdk_ebs::Client::from_conf(config)
    }

    fn create_mock_ec2_client() -> aws_sdk_ec2::Client {
        let config = Ec2Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new("us-east-1"))
            .build();
        aws_sdk_ec2::Client::from_conf(config)
    }

    fn create_mock_metadata_manager() -> Arc<MetadataManager> {
        let source_ec2 = create_mock_ec2_client();
        let source_ebs = create_mock_ebs_client();
        Arc::new(MetadataManager::new(source_ec2, source_ebs))
    }

    fn create_manager() -> SnapshotManager {
        SnapshotManager::new(
            create_mock_ebs_client(),
            create_mock_ebs_client(),
            create_mock_ec2_client(),
            create_mock_metadata_manager(),
            ReplicationConfig::default(),
        )
    }

    // --- Destination tag building tests ---

    fn metadata_with_tags(tags: Vec<(&str, &str)>) -> crate::metadata::SnapshotMetadata {
        crate::metadata::SnapshotMetadata {
            description: Some("a description".to_string()),
            tags: tags
                .into_iter()
                .map(|(k, v)| aws_sdk_ec2::types::Tag::builder().key(k).value(v).build())
                .collect(),
            volume_size: 8,
            block_size: crate::block_copier::EBS_BLOCK_SIZE,
            state: crate::metadata::SnapshotState::Completed,
        }
    }

    fn tag_pairs(tags: &[aws_sdk_ebs::types::Tag]) -> Vec<(String, String)> {
        tags.iter()
            .map(|t| {
                (
                    t.key().unwrap_or_default().to_string(),
                    t.value().unwrap_or_default().to_string(),
                )
            })
            .collect()
    }

    #[test]
    fn test_build_destination_tags_adds_commercial_arn() {
        let meta = metadata_with_tags(vec![]);
        let tags = build_destination_tags(&meta, "us-east-1", "snap-0123456789abcdef0");
        let pairs = tag_pairs(&tags);
        assert!(pairs.contains(&(
            "SourceSnapshotArn".to_string(),
            "arn:aws:ec2:us-east-1::snapshot/snap-0123456789abcdef0".to_string()
        )));
    }

    #[test]
    fn test_build_destination_tags_uses_correct_partition() {
        let meta = metadata_with_tags(vec![]);
        for (region, partition) in [
            ("us-gov-west-1", "aws-us-gov"),
            ("cn-north-1", "aws-cn"),
            ("eusc-de-east-1", "aws-eusc"),
        ] {
            let tags = build_destination_tags(&meta, region, "snap-abc123");
            let expected = format!("arn:{partition}:ec2:{region}::snapshot/snap-abc123");
            let pairs = tag_pairs(&tags);
            assert!(
                pairs.contains(&("SourceSnapshotArn".to_string(), expected.clone())),
                "expected {expected} in {pairs:?}"
            );
        }
    }

    #[test]
    fn test_build_destination_tags_filters_aws_reserved() {
        let meta = metadata_with_tags(vec![
            ("Name", "my-vol"),
            ("aws:ec2:fleet-id", "fleet-123"),
            ("Environment", "prod"),
        ]);
        let tags = build_destination_tags(&meta, "us-east-1", "snap-abc123");
        let pairs = tag_pairs(&tags);
        // User tags preserved.
        assert!(pairs.contains(&("Name".to_string(), "my-vol".to_string())));
        assert!(pairs.contains(&("Environment".to_string(), "prod".to_string())));
        // Reserved tag dropped.
        assert!(!pairs.iter().any(|(k, _)| k.starts_with("aws:")));
    }

    #[test]
    fn test_build_destination_tags_does_not_duplicate_description() {
        // Description is set as the snapshot's Description field, not a tag.
        let meta = metadata_with_tags(vec![]);
        let tags = build_destination_tags(&meta, "us-east-1", "snap-abc123");
        let pairs = tag_pairs(&tags);
        assert!(!pairs.iter().any(|(k, _)| k == "Description"));
    }

    // --- Block filtering tests ---

    fn resumed_state_with(indexes: &[i32]) -> ResumedState {
        use std::collections::HashSet;
        ResumedState {
            completed_indexes: indexes.iter().copied().collect::<HashSet<i32>>(),
            // Checksums are irrelevant to filtering; left empty on purpose.
            checksums: Vec::new(),
        }
    }

    fn blocks_0_to(n: i32) -> Vec<ChangedBlock> {
        (0..n)
            .map(|i| {
                ChangedBlock::Copy(crate::block_copier::BlockInfo {
                    index: i,
                    token: format!("token-{i}"),
                })
            })
            .collect()
    }

    #[test]
    fn test_block_filtering_removes_completed_indexes() {
        let state = resumed_state_with(&[0, 2, 4, 6, 8]);
        let remaining = filter_remaining_blocks(blocks_0_to(10), &state);

        let indexes: Vec<i32> = remaining.iter().map(|b| b.index()).collect();
        assert_eq!(indexes, vec![1, 3, 5, 7, 9]);
    }

    #[test]
    fn test_block_filtering_no_completed_returns_all() {
        let state = resumed_state_with(&[]);
        let remaining = filter_remaining_blocks(blocks_0_to(5), &state);
        assert_eq!(remaining.len(), 5);
    }

    #[test]
    fn test_block_filtering_all_completed_returns_empty() {
        let state = resumed_state_with(&[0, 1, 2, 3, 4]);
        let remaining = filter_remaining_blocks(blocks_0_to(5), &state);
        assert!(remaining.is_empty());
    }

    // --- Checksum merging tests ---

    #[test]
    fn test_checksum_merging_sorted_by_index() {
        let resumed = vec![
            BlockChecksum {
                index: 0,
                checksum: "c0".to_string(),
            },
            BlockChecksum {
                index: 4,
                checksum: "c4".to_string(),
            },
        ];
        let new = vec![
            BlockChecksum {
                index: 3,
                checksum: "c3".to_string(),
            },
            BlockChecksum {
                index: 1,
                checksum: "c1".to_string(),
            },
            BlockChecksum {
                index: 2,
                checksum: "c2".to_string(),
            },
        ];

        let all = merge_checksums(resumed, new);

        assert_eq!(all.len(), 5);
        for (i, c) in all.iter().enumerate() {
            assert_eq!(c.index, i as i32);
        }
    }

    #[test]
    fn test_checksum_merging_empty_resumed() {
        let new = vec![
            BlockChecksum {
                index: 1,
                checksum: "c1".to_string(),
            },
            BlockChecksum {
                index: 0,
                checksum: "c0".to_string(),
            },
        ];

        let all = merge_checksums(Vec::new(), new);

        assert_eq!(all.len(), 2);
        assert_eq!(all[0].index, 0);
        assert_eq!(all[1].index, 1);
    }

    // --- SnapshotStatus tests ---

    #[test]
    fn test_snapshot_status_pending_mapping() {
        let status = SnapshotStatus::from(Some(&aws_sdk_ebs::types::Status::Pending));
        assert_eq!(status, SnapshotStatus::Pending);
    }

    #[test]
    fn test_snapshot_status_completed_mapping() {
        let status = SnapshotStatus::from(Some(&aws_sdk_ebs::types::Status::Completed));
        assert_eq!(status, SnapshotStatus::Completed);
    }

    #[test]
    fn test_snapshot_status_error_mapping() {
        let status = SnapshotStatus::from(Some(&aws_sdk_ebs::types::Status::Error));
        assert_eq!(status, SnapshotStatus::Error);
    }

    #[test]
    fn test_snapshot_status_none_defaults_to_pending() {
        let status = SnapshotStatus::from(None);
        assert_eq!(status, SnapshotStatus::Pending);
    }

    #[test]
    fn test_start_snapshot_result_fields() {
        let result = StartSnapshotResult {
            snapshot_id: "snap-abc123".to_string(),
            status: SnapshotStatus::Pending,
        };
        assert_eq!(result.snapshot_id, "snap-abc123");
        assert_eq!(result.status, SnapshotStatus::Pending);
    }

    // --- copy_blocks_concurrently tests ---

    #[tokio::test]
    async fn test_copy_blocks_empty_list() {
        let manager = create_manager();
        let result = manager
            .copy_blocks_concurrently("snap-src", "snap-dst", vec![], 0, 0, None)
            .await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 0);
    }

    // --- Error type tests ---

    #[test]
    fn test_completion_failed_structure() {
        let error = SnapshotError::CompletionFailed {
            snapshot_id: "snap-123".to_string(),
            source: Box::new(std::io::Error::other("test error")),
        };
        assert!(error.to_string().contains("snap-123"));
    }

    #[test]
    fn test_wait_timeout_structure() {
        let error = SnapshotError::WaitTimeout {
            snapshot_id: "snap-456".to_string(),
            elapsed_secs: 1800,
        };
        let msg = error.to_string();
        assert!(msg.contains("snap-456"));
        assert!(msg.contains("1800"));
    }

    #[test]
    fn test_api_error_structure() {
        let error = SnapshotError::ApiError {
            message: "API call failed".to_string(),
        };
        assert!(error.to_string().contains("API call failed"));
    }

    #[test]
    fn test_from_copy_error_conversion() {
        let copy_error = CopyError::GetBlockFailed {
            block_index: 10,
            snapshot_id: "snap-xyz".to_string(),
            source: Box::new(std::io::Error::other("test error")),
        };
        let snapshot_error: SnapshotError = copy_error.into();
        match snapshot_error {
            SnapshotError::BlockCopyError { snapshot_id, .. } => {
                assert_eq!(snapshot_id, "snap-xyz");
            }
            _ => panic!("Expected BlockCopyError variant"),
        }
    }

    #[test]
    fn test_polling_backoff_progression() {
        // Mirrors the backoff in wait_for_completion: start at 2s, double each
        // step, cap at 15s. This checks the real schedule the code uses.
        const INITIAL: u64 = 2;
        const CAP: u64 = 15;
        let mut interval = INITIAL;
        let mut schedule = Vec::new();
        for _ in 0..6 {
            schedule.push(interval);
            interval = (interval * 2).min(CAP);
        }
        assert_eq!(schedule, vec![2, 4, 8, 15, 15, 15]);
    }

    // --- Snapshot manager creation ---

    #[test]
    fn test_snapshot_manager_creation() {
        let manager = create_manager();
        assert!(format!("{:?}", manager).contains("SnapshotManager"));
    }

    #[test]
    fn test_client_token_includes_encrypted_flag() {
        // The `encrypted` request must be part of the token so that
        // toggling `--encrypt` (default EBS key, so kms_key_id stays None) does
        // not reuse the same token for a different encryption request — which
        // EBS would reject with ConflictException against a pending snapshot.
        let m = create_manager();

        let plain = m.generate_client_token("snap-abc", "us-west-2", None, &None, &None);
        let encrypted = m.generate_client_token("snap-abc", "us-west-2", Some(true), &None, &None);
        assert_ne!(plain, encrypted, "encrypted flag must change the token");

        // Deterministic for identical inputs, and a 64-char hex digest.
        assert_eq!(
            plain,
            m.generate_client_token("snap-abc", "us-west-2", None, &None, &None)
        );
        assert_eq!(plain.len(), 64);

        // "No request" and an explicit `false` are different requests.
        let explicit_false =
            m.generate_client_token("snap-abc", "us-west-2", Some(false), &None, &None);
        assert_ne!(plain, explicit_false);

        // A different KMS key yields a different token.
        let k1 = m.generate_client_token(
            "snap-abc",
            "us-west-2",
            Some(true),
            &Some("arn:k1".into()),
            &None,
        );
        let k2 = m.generate_client_token(
            "snap-abc",
            "us-west-2",
            Some(true),
            &Some("arn:k2".into()),
            &None,
        );
        assert_ne!(k1, k2);
    }

    #[test]
    fn test_kms_access_denied_error_message() {
        use std::error::Error;
        let error = SnapshotError::KmsAccessDenied {
            key_arn: "arn:aws:kms:us-west-2:123456789012:key/test-key".to_string(),
            source: Box::new(std::io::Error::other("kms:GenerateDataKey explicit deny")),
        };
        let msg = error.to_string();
        assert!(msg.contains("arn:aws:kms:us-west-2:123456789012:key/test-key"));
        assert!(msg.contains("kms:CreateGrant"));
        assert!(msg.contains("kms:Decrypt"));
        // The underlying KMS cause is preserved so report_error can print it.
        assert!(error.source().is_some());
        assert!(error
            .source()
            .unwrap()
            .to_string()
            .contains("kms:GenerateDataKey"));
    }

    #[test]
    fn test_kms_access_denied_error_debug() {
        let error = SnapshotError::KmsAccessDenied {
            key_arn: "arn:aws:kms:us-west-2:123456789012:key/test-key".to_string(),
            source: Box::new(std::io::Error::other("underlying kms error")),
        };
        let debug = format!("{:?}", error);
        assert!(debug.contains("KmsAccessDenied"));
    }

    // --- StartSnapshot error classification ---

    #[test]
    fn test_denial_classifier_ebs_validation_envelope_kms_deny_is_kms() {
        // EBS returns a top-level `ValidationException` whose message embeds a
        // KMS AccessDeniedException. The access-denied + KMS signals live in the
        // MESSAGE, not the code.
        let code = "ValidationException";
        let message = "Invalid parameters for a dependent service: \
             com.amazonaws.services.kms.model.AWSKMSException: User: \
             arn:aws:sts::123456789012:assumed-role/ExampleRole/session is not authorized to \
             perform: kms:GenerateDataKey on resource: \
             arn:aws:kms:eu-west-3:123456789012:key/1234abcd-12ab-34cd-56ef-1234567890ab with an explicit deny \
             in a resource-based policy (Service: AWSKMS; Status Code: 400; \
             Error Code: AccessDeniedException)";
        assert!(
            start_snapshot_denial_is_kms(code, message, true),
            "EBS ValidationException wrapping a KMS AccessDeniedException must classify as KMS"
        );
        // Also holds without the explicit-key hint, because the message itself
        // carries both the access-denied and KMS signals.
        assert!(start_snapshot_denial_is_kms(code, message, false));
    }

    #[test]
    fn test_denial_classifier_plain_iam_is_not_kms() {
        // A bare AccessDenied with no KMS/key hint and no explicit key is a plain
        // IAM/endpoint denial — must NOT be reported as a KMS error.
        assert!(!start_snapshot_denial_is_kms(
            "AccessDeniedException",
            "User is not authorized to perform ebs:StartSnapshot",
            false,
        ));
    }

    #[test]
    fn test_denial_classifier_kms_in_message_is_kms() {
        assert!(start_snapshot_denial_is_kms(
            "AccessDeniedException",
            "The ciphertext refers to a KMS key that does not exist",
            false,
        ));
    }

    #[test]
    fn test_denial_classifier_key_in_message_is_kms() {
        assert!(start_snapshot_denial_is_kms(
            "AccessDenied",
            "not authorized to use the key",
            false,
        ));
    }

    #[test]
    fn test_denial_classifier_explicit_kms_key_makes_denial_kms() {
        // Even a generic AccessDenied is treated as KMS when the user supplied a
        // target key, since the denial is almost certainly about that key.
        assert!(start_snapshot_denial_is_kms(
            "AccessDeniedException",
            "access denied",
            true,
        ));
    }

    #[test]
    fn test_denial_classifier_non_denial_is_not_kms() {
        // A throttling / validation error is never KMS, regardless of key.
        assert!(!start_snapshot_denial_is_kms(
            "ThrottlingException",
            "rate exceeded",
            true,
        ));
        assert!(!start_snapshot_denial_is_kms(
            "ValidationException",
            "Snapshot is already completed",
            false,
        ));
    }

    #[test]
    fn test_encrypted_param_omitted_with_parent() {
        // Sending Encrypted (even false) with a parent is rejected by EBS.
        assert_eq!(start_snapshot_encrypted_param(true, Some(false)), None);
        assert_eq!(start_snapshot_encrypted_param(true, Some(true)), None);
        assert_eq!(start_snapshot_encrypted_param(true, None), None);
    }

    #[test]
    fn test_encrypted_param_sent_without_parent() {
        assert_eq!(
            start_snapshot_encrypted_param(false, Some(false)),
            Some(false)
        );
        assert_eq!(
            start_snapshot_encrypted_param(false, Some(true)),
            Some(true)
        );
    }

    #[test]
    fn test_encrypted_param_omitted_when_not_requested() {
        // No flags: nothing is sent, so the target account's
        // encryption-by-default setting decides. Sending `false` would make
        // EBS fail the request when that setting is enabled.
        assert_eq!(start_snapshot_encrypted_param(false, None), None);
    }

    #[test]
    fn test_start_snapshot_failed_preserves_source() {
        use std::error::Error;
        let error = SnapshotError::StartSnapshotFailed {
            target_region: "eu-west-3".to_string(),
            source: Box::new(std::io::Error::other("underlying sdk error")),
        };
        assert!(error.to_string().contains("eu-west-3"));
        assert!(error.source().is_some());
    }
}
