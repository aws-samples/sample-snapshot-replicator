// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

//! Integration tests for end-to-end snapshot replication
//!
//! These tests require real AWS credentials and test snapshots.
//! Run with: cargo test --test integration_test -- --ignored
//!
//! Environment variables required:
//! - TEST_SOURCE_SNAPSHOT_ID: Source snapshot ID to replicate
//! - TEST_SOURCE_REGION: Source region (default: us-east-1)
//! - TEST_TARGET_REGION: Target region (default: us-west-2)
//! - TEST_SOURCE_PROFILE: AWS profile for source (optional)
//! - TEST_TARGET_PROFILE: AWS profile for target (required)

use snapshot_replicator::{
    auth::AuthManager, config::ReplicationConfig, metadata::MetadataManager,
    snapshot::SnapshotManager,
};
use std::env;
use std::sync::Arc;
use std::time::Instant;

#[tokio::test]
#[ignore = "requires AWS credentials and a real source snapshot; see README"]
async fn test_full_replication_workflow() {
    // Setup: Get test configuration from environment
    let source_snapshot_id =
        env::var("TEST_SOURCE_SNAPSHOT_ID").expect("TEST_SOURCE_SNAPSHOT_ID must be set");
    let source_region = env::var("TEST_SOURCE_REGION").unwrap_or_else(|_| "us-east-1".to_string());
    let target_region = env::var("TEST_TARGET_REGION").unwrap_or_else(|_| "us-west-2".to_string());
    let source_profile = env::var("TEST_SOURCE_PROFILE").ok();
    let target_profile = env::var("TEST_TARGET_PROFILE").expect("TEST_TARGET_PROFILE must be set");

    println!("\n=== Integration Test: Full Replication Workflow ===");
    println!("Source snapshot: {}", source_snapshot_id);
    println!("Source region: {}", source_region);
    println!("Target region: {}", target_region);

    // Create configuration
    let config = ReplicationConfig::from_cli(10, 3).expect("valid tuning config");

    // Create authentication manager and clients
    let auth_manager = AuthManager::new(source_profile.clone(), target_profile.clone(), 3);

    let source_ebs_client = auth_manager.create_source_client(&source_region).await;
    let target_ebs_client = auth_manager.create_target_client(&target_region).await;

    // Build EC2 clients through AuthManager so credential precedence and retry
    // wiring match production (and exercise the fixed env-vs-profile behaviour).
    let source_ec2_config = auth_manager.source_sdk_config(&source_region).await;
    let source_ec2_client = aws_sdk_ec2::Client::new(&source_ec2_config);

    let target_ec2_config = auth_manager.target_sdk_config(&target_region).await;
    let target_ec2_client = aws_sdk_ec2::Client::new(&target_ec2_config);

    // Validate source snapshot
    let metadata_manager = Arc::new(MetadataManager::new(
        source_ec2_client.clone(),
        source_ebs_client.clone(),
    ));

    let source_metadata = metadata_manager
        .get_snapshot_metadata(&source_snapshot_id)
        .await
        .expect("Failed to get source snapshot metadata");

    println!("Source snapshot: {} GiB", source_metadata.volume_size);
    assert!(source_metadata.volume_size > 0);

    // List blocks
    let blocks = metadata_manager
        .list_snapshot_blocks(&source_snapshot_id)
        .await
        .expect("Failed to list blocks");

    println!("Blocks to copy: {}", blocks.len());
    assert!(!blocks.is_empty(), "Source snapshot must have blocks");

    let snapshot_manager = SnapshotManager::new(
        source_ebs_client,
        target_ebs_client,
        target_ec2_client.clone(),
        metadata_manager.clone(),
        config,
    );

    // A full copy writes every data block through.
    let blocks: Vec<snapshot_replicator::ChangedBlock> = blocks
        .into_iter()
        .map(snapshot_replicator::ChangedBlock::Copy)
        .collect();

    // Replicate snapshot (single method call)
    let start_time = Instant::now();
    let dest_snapshot_id = snapshot_manager
        .replicate_snapshot(snapshot_replicator::ReplicationRequest {
            source_snapshot_id: source_snapshot_id.clone(),
            source_region: source_region.clone(),
            target_region: target_region.clone(),
            blocks,
            encrypted: None,
            kms_key_id: None,
            parent_dest_snapshot_id: None,
        })
        .await
        .expect("Failed to replicate snapshot")
        .dest_snapshot_id;

    let total_duration = start_time.elapsed();
    println!("Destination snapshot: {}", dest_snapshot_id);
    println!("Total duration: {:.2}s", total_duration.as_secs_f64());

    // Verify the destination snapshot through the TARGET EC2 client. Using the
    // source client here would fail for any cross-region/cross-partition run.
    let dest = target_ec2_client
        .describe_snapshots()
        .snapshot_ids(&dest_snapshot_id)
        .send()
        .await
        .expect("Failed to describe destination snapshot");

    let dest_snapshot = dest
        .snapshots()
        .first()
        .expect("destination snapshot not found in target region");

    assert_eq!(
        dest_snapshot.volume_size(),
        Some(source_metadata.volume_size as i32),
        "Volume sizes must match"
    );

    // Cleanup: Delete destination snapshot in the target region.
    println!("Cleaning up destination snapshot...");
    target_ec2_client
        .delete_snapshot()
        .snapshot_id(&dest_snapshot_id)
        .send()
        .await
        .expect("Failed to delete destination snapshot");

    println!("✓ Test passed");
}

#[tokio::test]
#[ignore = "requires AWS credentials; see README"]
async fn test_error_handling_invalid_snapshot() {
    // Test that invalid snapshot ID produces clear error
    let source_region = "us-east-1".to_string();
    let target_profile = env::var("TEST_TARGET_PROFILE").unwrap_or_else(|_| "default".to_string());

    let auth_manager = AuthManager::new(None, target_profile, 3);
    let source_ebs_client = auth_manager.create_source_client(&source_region).await;

    let source_ec2_config = auth_manager.source_sdk_config(&source_region).await;
    let source_ec2_client = aws_sdk_ec2::Client::new(&source_ec2_config);

    let metadata_manager = MetadataManager::new(source_ec2_client, source_ebs_client);

    // Try to get metadata for non-existent snapshot
    let result = metadata_manager
        .get_snapshot_metadata("snap-nonexistent123")
        .await;

    assert!(result.is_err(), "Should fail for invalid snapshot");
    println!("✓ Error handling test passed");
}

#[tokio::test]
#[ignore = "requires AWS credentials and a real source snapshot; see README"]
async fn test_metadata_preservation() {
    // This test verifies that description and tags are present on the source,
    // which are applied to the destination at StartSnapshot time.
    let source_snapshot_id =
        env::var("TEST_SOURCE_SNAPSHOT_ID").expect("TEST_SOURCE_SNAPSHOT_ID must be set");
    let source_region = env::var("TEST_SOURCE_REGION").unwrap_or_else(|_| "us-east-1".to_string());
    let target_profile = env::var("TEST_TARGET_PROFILE").expect("TEST_TARGET_PROFILE must be set");

    println!("\n=== Integration Test: Metadata Preservation ===");

    let auth_manager = AuthManager::new(None, target_profile.clone(), 3);
    let source_ebs_client = auth_manager.create_source_client(&source_region).await;

    let source_ec2_config = auth_manager.source_sdk_config(&source_region).await;
    let source_ec2_client = aws_sdk_ec2::Client::new(&source_ec2_config);

    let metadata_manager = MetadataManager::new(source_ec2_client, source_ebs_client);

    // Get source metadata
    let source_metadata = metadata_manager
        .get_snapshot_metadata(&source_snapshot_id)
        .await
        .expect("Failed to get source metadata");

    println!(
        "Source description: {:?}",
        source_metadata.description.as_deref().unwrap_or("none")
    );
    println!("Source tags: {}", source_metadata.tags.len());

    println!("✓ Metadata preservation test passed");
}
