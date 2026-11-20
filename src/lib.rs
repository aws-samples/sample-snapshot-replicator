// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

//! Snapshot Replicator Library
//!
//! This library provides functionality for replicating EBS snapshots across AWS partitions
//! using EBS Direct APIs. It supports cross-partition authentication via separate AWS
//! profiles (one per partition) and parallel block-level copying for high throughput.

pub mod auth;
pub mod block_copier;
pub mod checksum;
pub mod cli;
pub mod config;
pub mod metadata;
pub mod progress;
pub mod region;
pub mod snapshot;
pub mod state;

// Re-export commonly used types
pub use auth::AuthManager;
pub use block_copier::{BlockChecksum, BlockCopier, BlockInfo, ChangedBlock, CopyError};
pub use checksum::{aggregate_checksum, ChecksumError};
pub use cli::{CliArgs, ValidationError};
pub use config::{ConfigError, ReplicationConfig};
pub use metadata::{MetadataError, MetadataManager, SnapshotMetadata, SnapshotState};
pub use progress::ProgressReporter;
pub use region::{partition_for_region, validate_region, RegionError};
pub use snapshot::{
    DestinationEncryption, DestinationSpec, ReplicationOutcome, ReplicationRequest, SnapshotError,
    SnapshotManager, SnapshotStatus, StartSnapshotResult,
};
pub use state::{ResumedState, StateFile};
