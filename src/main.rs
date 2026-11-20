// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

use snapshot_replicator::{
    auth::AuthManager, block_copier::EBS_BLOCK_SIZE, cli, config, metadata::MetadataManager,
    region::validate_region, snapshot::SnapshotManager,
};
use std::process;
use std::sync::Arc;
use std::time::Instant;

/// Shown when neither `--encrypt` nor `--kms-key-id` is given.
const NO_ENCRYPTION_WARNING: &str = "Warning, encryption is not set. This default to the default encryption settings on the target AWS Account/Region.";

/// Print an error and its full `source()` chain to stderr.
///
/// The CLI's typed errors keep the underlying cause as `#[source]` (for example
/// the real AWS `SdkError` behind a failed `PutSnapshotBlock`), but a plain
/// `{}` only prints the top-level message. Walking the chain surfaces the actual
/// cause — AccessDenied, a KMS/grant error, ThrottlingException, a
/// ValidationException, etc. — which is exactly what the operator needs when a
/// cross-partition IAM/KMS/endpoint problem is the real failure.
///
/// Consecutive links whose `Display` text is identical are collapsed: the AWS
/// SDK's modeled errors nest an inner node that stringifies the same as its
/// parent, which would otherwise print the same `caused by:` line twice.
fn report_error(context: &str, err: &(dyn std::error::Error + 'static)) {
    eprintln!("Error {context}: {err}");
    let mut previous = err.to_string();
    let mut source = err.source();
    while let Some(cause) = source {
        let text = cause.to_string();
        if text != previous {
            eprintln!("  caused by: {text}");
            previous = text;
        }
        source = cause.source();
    }
}

#[tokio::main]
async fn main() {
    // Initialize logging
    env_logger::init();
    log::info!("Snapshot Replicator starting");

    // Parse and validate CLI arguments
    let args = cli::parse_args();
    if let Err(e) = cli::validate_args(&args) {
        eprintln!("Error: {}", e);
        process::exit(1);
    }

    log::info!(
        "Replicating {} to region {}",
        args.snapshot_id,
        args.target_region
    );

    // Explicit encryption request: --kms-key-id implies --encrypt. None means
    // no request: Encrypted/KmsKeyArn are omitted and the target account's
    // encryption-by-default setting decides (the source's state is irrelevant).
    let encrypt_override = if args.encrypt || args.kms_key_id.is_some() {
        Some(true)
    } else {
        None
    };

    // Warn when encryption was not requested. Skipped for incremental copies:
    // those cannot take --encrypt/--kms-key-id (validate_args rejects them)
    // because the child inherits encryption from the parent destination.
    if encrypt_override.is_none() && args.parent_snapshot_id.is_none() {
        eprintln!("{}", NO_ENCRYPTION_WARNING);
    }

    // Build tuning configuration from CLI flags
    let config = match config::ReplicationConfig::from_cli(args.concurrency, args.max_attempts) {
        Ok(cfg) => {
            log::info!(
                "Tuning: concurrency={}, max_attempts={}",
                cfg.concurrency,
                args.max_attempts
            );
            cfg
        }
        Err(e) => {
            eprintln!("Error: {}", e);
            process::exit(1);
        }
    };

    // Validate region formats
    log::info!("Validating target region");
    if let Err(e) = validate_region(&args.target_region) {
        eprintln!("Error: {}", e);
        process::exit(1);
    }

    let source_region = args.source_region.clone();
    log::info!("Validating source region: {}", source_region);
    if let Err(e) = validate_region(&source_region) {
        eprintln!("Error: {}", e);
        process::exit(1);
    }

    // Create authentication manager and AWS clients
    log::info!("Creating AWS clients");
    let auth_manager = AuthManager::new(
        args.source_profile.clone(),
        args.target_profile.clone(),
        args.max_attempts,
    );

    let source_ebs_client = auth_manager.create_source_client(&source_region).await;
    let target_ebs_client = auth_manager.create_target_client(&args.target_region).await;

    // Create EC2 clients for metadata operations, reusing the same credential and
    // retry wiring as the EBS clients so profile precedence and adaptive retry are
    // consistent across both services in each partition.
    let source_ec2_config = auth_manager.source_sdk_config(&source_region).await;
    let source_ec2_client = aws_sdk_ec2::Client::new(&source_ec2_config);

    let target_ec2_config = auth_manager.target_sdk_config(&args.target_region).await;
    let target_ec2_client = aws_sdk_ec2::Client::new(&target_ec2_config);

    // Snapshot copy workflow
    let snapshot_id = &args.snapshot_id;

    // Create metadata manager and snapshot manager
    log::info!("Creating managers");
    let metadata_manager = Arc::new(MetadataManager::new(
        source_ec2_client.clone(),
        source_ebs_client.clone(),
    ));

    let snapshot_manager = SnapshotManager::new(
        source_ebs_client,
        target_ebs_client,
        target_ec2_client,
        metadata_manager.clone(),
        config,
    );

    // Get source snapshot metadata for display
    log::info!("Validating source snapshot");
    match metadata_manager.get_snapshot_metadata(snapshot_id).await {
        Ok(metadata) => {
            log::info!(
                "Source snapshot validated: {} GiB, {} blocks",
                metadata.volume_size,
                metadata.volume_size * 1024 * 1024 * 1024 / metadata.block_size as i64
            );
            println!(
                "Source snapshot: {} ({} GiB)",
                snapshot_id, metadata.volume_size
            );
        }
        Err(e) => {
            report_error("validating source snapshot", &e);
            process::exit(1);
        }
    };

    // List the blocks to copy. For an incremental copy (parent IDs supplied),
    // only the blocks that changed between the parent and the source snapshot are
    // transferred; otherwise every data block is copied.
    let blocks = match &args.parent_snapshot_id {
        Some(parent_source_id) => {
            log::info!(
                "Incremental copy: listing changed blocks between {} and {}",
                parent_source_id,
                snapshot_id
            );
            match metadata_manager
                .list_changed_blocks(parent_source_id, snapshot_id)
                .await
            {
                Ok(blocks) => {
                    log::info!("Found {} changed blocks to copy", blocks.len());
                    println!("Changed blocks to copy: {}", blocks.len());
                    blocks
                }
                Err(e) => {
                    report_error("listing changed blocks", &e);
                    process::exit(1);
                }
            }
        }
        None => {
            log::info!("Full copy: listing all blocks from source snapshot");
            match metadata_manager.list_snapshot_blocks(snapshot_id).await {
                Ok(blocks) => {
                    log::info!("Found {} data blocks to copy", blocks.len());
                    println!("Blocks to copy: {}", blocks.len());
                    // A full copy writes every data block through; wrap each in
                    // ChangedBlock::Copy so the copy pipeline is uniform.
                    blocks
                        .into_iter()
                        .map(snapshot_replicator::ChangedBlock::Copy)
                        .collect()
                }
                Err(e) => {
                    report_error("listing blocks", &e);
                    process::exit(1);
                }
            }
        }
    };

    // Replicate snapshot (single method call)
    log::info!("Starting snapshot replication");
    let start_time = Instant::now();

    // Number of data blocks this run was asked to copy. For a sparse full copy
    // this is far fewer than the whole volume; for an incremental copy it is
    // only the changed blocks. Captured here because `blocks` is moved below.
    let blocks_to_copy = blocks.len();

    let outcome = match snapshot_manager
        .replicate_snapshot(snapshot_replicator::ReplicationRequest {
            source_snapshot_id: snapshot_id.clone(),
            source_region: source_region.clone(),
            target_region: args.target_region.clone(),
            blocks,
            encrypted: encrypt_override,
            kms_key_id: args.kms_key_id.clone(),
            parent_dest_snapshot_id: args.parent_dest_snapshot_id.clone(),
        })
        .await
    {
        Ok(outcome) => {
            log::info!(
                "Snapshot replication completed: {} (reused_existing={})",
                outcome.dest_snapshot_id,
                outcome.reused_existing
            );
            outcome
        }
        Err(e) => {
            report_error("replicating snapshot", &e);
            process::exit(1);
        }
    };

    // Display final summary
    let duration = start_time.elapsed();

    if outcome.reused_existing {
        // The destination was already complete (idempotent reuse); this run
        // copied nothing, so reporting a block count / throughput would be
        // misleading. Report the reuse instead.
        log::info!(
            "Destination {} already existed and was reused in {:.2}s",
            outcome.dest_snapshot_id,
            duration.as_secs_f64()
        );
        println!("\n✓ Already replicated — reused existing snapshot (no copy performed)");
        println!("  Destination snapshot: {}", outcome.dest_snapshot_id);
        println!("  Duration: {:.2}s", duration.as_secs_f64());
        return;
    }

    // Throughput is based on the data actually copied (blocks this run was asked
    // to transfer), not the full volume size. A sparse or incremental copy moves
    // far fewer bytes than the volume is large, so using volume size here would
    // report a wildly inflated rate. Note the duration also includes fixed costs
    // (block listing and the completion poll), so this is an end-to-end rate
    // rather than a pure transfer rate.
    let bytes_copied = blocks_to_copy as u64 * EBS_BLOCK_SIZE as u64;
    let throughput_mbps = (bytes_copied as f64 / duration.as_secs_f64()) / (1024.0 * 1024.0);

    log::info!(
        "Replication completed in {:.2}s, {} blocks copied, throughput: {:.2} MB/s",
        duration.as_secs_f64(),
        blocks_to_copy,
        throughput_mbps
    );

    println!("\n✓ Replication completed successfully!");
    println!("  Destination snapshot: {}", outcome.dest_snapshot_id);
    // Report encryption only when EC2 states the destination is encrypted.
    if let Some(enc) = &outcome.encryption {
        if enc.encrypted == Some(true) {
            match &enc.kms_key_id {
                Some(key) => println!("  Encrypted: yes (KMS key: {})", key),
                None => println!("  Encrypted: yes"),
            }
        }
    }
    println!(
        "  Data copied: {} blocks ({:.2} MiB)",
        blocks_to_copy,
        bytes_copied as f64 / (1024.0 * 1024.0)
    );
    println!("  Duration: {:.2}s", duration.as_secs_f64());
    println!("  Throughput: {:.2} MB/s", throughput_mbps);
}
