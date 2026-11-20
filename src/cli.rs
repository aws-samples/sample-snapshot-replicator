// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

//! CLI argument parsing and validation
//!
//! This module handles command-line interface argument parsing using clap,
//! and provides validation for snapshot IDs and AWS regions.

use clap::Parser;
use thiserror::Error;

/// Snapshot Replicator - Copy EBS snapshots across AWS partitions
#[derive(Parser, Debug)]
#[command(name = "snapshot-replicator", version)]
#[command(about = "Replicate EBS snapshots across AWS partitions", long_about = None)]
pub struct CliArgs {
    /// Source EBS snapshot identifier (e.g., snap-0123456789abcdef0)
    #[arg(short = 's', long)]
    pub snapshot_id: String,

    /// Source AWS region where the snapshot lives (e.g., us-east-1).
    #[arg(long = "source-region")]
    pub source_region: String,

    /// Target AWS region for the replicated snapshot (e.g., us-west-2)
    #[arg(short = 't', long)]
    pub target_region: String,

    /// AWS profile name for the source partition (optional, defaults to default profile)
    #[arg(long = "source-profile")]
    pub source_profile: Option<String>,

    /// AWS profile name for the target partition (required)
    #[arg(short = 'p', long)]
    pub target_profile: String,

    /// Number of concurrent block copy operations
    #[arg(short = 'c', long, default_value_t = 100)]
    pub concurrency: usize,

    /// Maximum attempts per block API call for the AWS SDK's adaptive retryer
    /// (first try + retries). The copy makes a single pass and fails fast on the
    /// first block error that survives these retries.
    #[arg(long, default_value_t = 5)]
    pub max_attempts: u32,

    /// Encrypt the destination snapshot
    #[arg(long)]
    pub encrypt: bool,

    /// KMS key ARN for destination snapshot encryption (implies --encrypt)
    #[arg(long)]
    pub kms_key_id: Option<String>,

    /// Parent source snapshot ID for incremental copy (the previously replicated
    /// snapshot of the same volume). Requires --parent-dest-snapshot-id.
    #[arg(long)]
    pub parent_snapshot_id: Option<String>,

    /// Destination snapshot ID that the parent source snapshot was replicated to.
    /// Requires --parent-snapshot-id.
    #[arg(long)]
    pub parent_dest_snapshot_id: Option<String>,
}

/// Validation errors for CLI arguments
#[derive(Debug, Error)]
pub enum ValidationError {
    #[error("Invalid snapshot ID: must be 'snap-' followed by lowercase hex digits (e.g. snap-0123456789abcdef0)")]
    InvalidSnapshotId,

    #[error("Invalid region format: expected pattern like 'us-west-2'")]
    InvalidRegion,

    #[error("Invalid KMS key ARN: must match 'arn:aws:kms:<region>:<account>:key/<key-id>'")]
    InvalidKmsKeyArn,

    #[error("--parent-snapshot-id and --parent-dest-snapshot-id must be provided together")]
    IncompleteParentArgs,

    #[error("Invalid parent snapshot ID: must be 'snap-' followed by lowercase hex digits")]
    InvalidParentSnapshotId,

    #[error(
        "--encrypt and --kms-key-id cannot be combined with --parent-snapshot-id: \
         an incremental snapshot inherits its encryption from the parent destination snapshot"
    )]
    EncryptionWithParent,
}

/// Parse command-line arguments
pub fn parse_args() -> CliArgs {
    CliArgs::parse()
}

/// Maximum snapshot ID length accepted by the EBS direct APIs.
const MAX_SNAPSHOT_ID_LEN: usize = 64;

/// Returns true if `id` matches the EBS API pattern `^snap-[0-9a-f]+$`
/// (at most 64 characters).
fn is_valid_snapshot_id(id: &str) -> bool {
    match id.strip_prefix("snap-") {
        Some(hex) => {
            !hex.is_empty()
                && id.len() <= MAX_SNAPSHOT_ID_LEN
                && hex.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
        }
        None => false,
    }
}

/// Validate snapshot ID format
///
/// Accepts the EBS API pattern `^snap-[0-9a-f]+$`. Rejecting anything else up
/// front also keeps unexpected characters (such as path separators) out of the
/// state file name, which is derived from snapshot IDs.
pub fn validate_snapshot_id(id: &str) -> Result<(), ValidationError> {
    if is_valid_snapshot_id(id) {
        Ok(())
    } else {
        Err(ValidationError::InvalidSnapshotId)
    }
}

/// Validate KMS key ARN format
///
/// Expected: `arn:<partition>:kms:<region>:<account-id>:key/<key-id>`, where
/// `<partition>` is one of the known AWS partitions (for example `aws`,
/// `aws-cn`, `aws-us-gov`, `aws-eusc`).
pub fn validate_kms_key_arn(arn: &str) -> Result<(), ValidationError> {
    // The partitions AWS currently defines (see the aws-partitions data set).
    const KNOWN_PARTITIONS: [&str; 8] = [
        "aws",
        "aws-cn",
        "aws-us-gov",
        "aws-iso",
        "aws-iso-b",
        "aws-iso-e",
        "aws-iso-f",
        "aws-eusc",
    ];

    let parts: Vec<&str> = arn.splitn(6, ':').collect();
    if parts.len() != 6
        || parts[0] != "arn"
        || !KNOWN_PARTITIONS.contains(&parts[1])
        || parts[2] != "kms"
        || parts[3].is_empty()
        || parts[4].is_empty()
        || !parts[5].starts_with("key/")
    {
        return Err(ValidationError::InvalidKmsKeyArn);
    }
    Ok(())
}

/// Validate CLI arguments
pub fn validate_args(args: &CliArgs) -> Result<(), ValidationError> {
    validate_snapshot_id(&args.snapshot_id)?;
    crate::region::validate_region(&args.source_region)
        .map_err(|_| ValidationError::InvalidRegion)?;
    crate::region::validate_region(&args.target_region)
        .map_err(|_| ValidationError::InvalidRegion)?;

    if let Some(kms_key_id) = &args.kms_key_id {
        validate_kms_key_arn(kms_key_id)?;
    }

    validate_parent_args(&args.parent_snapshot_id, &args.parent_dest_snapshot_id)?;

    // EBS rejects Encrypted together with ParentSnapshotId, and the child
    // inherits the parent's encryption, so these flags are meaningless here.
    if args.parent_snapshot_id.is_some() && (args.encrypt || args.kms_key_id.is_some()) {
        return Err(ValidationError::EncryptionWithParent);
    }

    Ok(())
}

/// Validate the incremental-copy parent arguments.
///
/// Both must be supplied together (or neither), and both must be snapshot IDs.
pub fn validate_parent_args(
    parent_snapshot_id: &Option<String>,
    parent_dest_snapshot_id: &Option<String>,
) -> Result<(), ValidationError> {
    match (parent_snapshot_id, parent_dest_snapshot_id) {
        (None, None) => Ok(()),
        (Some(src), Some(dest)) => {
            for id in [src, dest] {
                if !is_valid_snapshot_id(id) {
                    return Err(ValidationError::InvalidParentSnapshotId);
                }
            }
            Ok(())
        }
        _ => Err(ValidationError::IncompleteParentArgs),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_required_args_parsed() {
        let result = CliArgs::try_parse_from([
            "program",
            "--snapshot-id",
            "snap-12345",
            "--source-region",
            "us-east-1",
            "--target-region",
            "us-west-2",
            "--target-profile",
            "prod",
        ]);

        assert!(result.is_ok());
        let args = result.unwrap();
        assert_eq!(args.snapshot_id, "snap-12345".to_string());
        assert_eq!(args.source_region, "us-east-1");
        assert_eq!(args.target_region, "us-west-2");
        assert_eq!(args.target_profile, "prod");
        assert_eq!(args.source_profile, None);
        // Tuning flags fall back to their defaults.
        assert_eq!(args.concurrency, 100);
        assert_eq!(args.max_attempts, 5);
    }

    #[test]
    fn test_optional_source_profile() {
        let result = CliArgs::try_parse_from([
            "program",
            "--snapshot-id",
            "snap-12345",
            "--source-region",
            "us-east-1",
            "--target-region",
            "us-west-2",
            "--source-profile",
            "commercial",
            "--target-profile",
            "govcloud",
        ]);

        assert!(result.is_ok());
        let args = result.unwrap();
        assert_eq!(args.source_profile, Some("commercial".to_string()));
    }

    #[test]
    fn test_tuning_flags_parsed() {
        let result = CliArgs::try_parse_from([
            "program",
            "--snapshot-id",
            "snap-12345",
            "--source-region",
            "us-east-1",
            "--target-region",
            "us-west-2",
            "--target-profile",
            "prod",
            "--concurrency",
            "200",
            "--max-attempts",
            "8",
        ]);

        assert!(result.is_ok());
        let args = result.unwrap();
        assert_eq!(args.concurrency, 200);
        assert_eq!(args.max_attempts, 8);
    }

    #[test]
    fn test_source_region_flag_parsed() {
        let result = CliArgs::try_parse_from([
            "program",
            "--snapshot-id",
            "snap-12345",
            "--source-region",
            "eu-west-1",
            "--target-region",
            "us-west-2",
            "--target-profile",
            "prod",
        ]);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().source_region, "eu-west-1".to_string());
    }

    #[test]
    fn test_all_optional_args() {
        let result = CliArgs::try_parse_from([
            "program",
            "--snapshot-id",
            "snap-12345",
            "--source-region",
            "us-east-1",
            "--target-region",
            "us-west-2",
            "--source-profile",
            "commercial",
            "--target-profile",
            "govcloud",
            "--concurrency",
            "50",
            "--max-attempts",
            "3",
        ]);

        assert!(result.is_ok());
        let args = result.unwrap();
        assert_eq!(args.source_profile, Some("commercial".to_string()));
        assert_eq!(args.concurrency, 50);
        assert_eq!(args.max_attempts, 3);
    }

    #[test]
    fn test_no_optional_args() {
        let result = CliArgs::try_parse_from([
            "program",
            "--snapshot-id",
            "snap-12345",
            "--source-region",
            "us-east-1",
            "--target-region",
            "us-west-2",
            "--target-profile",
            "prod",
        ]);

        assert!(result.is_ok());
        let args = result.unwrap();
        assert_eq!(args.source_profile, None);
        assert_eq!(args.concurrency, 100);
        assert_eq!(args.max_attempts, 5);
    }

    #[test]
    fn test_missing_snapshot_id() {
        let result = CliArgs::try_parse_from([
            "program",
            "--source-region",
            "us-east-1",
            "--target-region",
            "us-west-2",
            "--target-profile",
            "prod",
        ]);

        assert!(result.is_err());
    }

    #[test]
    fn test_missing_source_region() {
        let result = CliArgs::try_parse_from([
            "program",
            "--snapshot-id",
            "snap-12345",
            "--target-region",
            "us-west-2",
            "--target-profile",
            "prod",
        ]);

        assert!(result.is_err());
    }

    #[test]
    fn test_missing_target_region() {
        let result = CliArgs::try_parse_from([
            "program",
            "--snapshot-id",
            "snap-12345",
            "--source-region",
            "us-east-1",
            "--target-profile",
            "prod",
        ]);

        assert!(result.is_err());
    }

    #[test]
    fn test_missing_target_profile() {
        let result = CliArgs::try_parse_from([
            "program",
            "--snapshot-id",
            "snap-12345",
            "--source-region",
            "us-east-1",
            "--target-region",
            "us-west-2",
        ]);

        assert!(result.is_err());
    }

    #[test]
    fn test_all_args_missing() {
        let result = CliArgs::try_parse_from(["program"]);

        assert!(result.is_err());
    }

    // ====== Validation Tests ======

    #[test]
    fn test_valid_snapshot_id_with_correct_prefix() {
        let result = validate_snapshot_id("snap-0123456789abcdef0");
        assert!(result.is_ok());
    }

    #[test]
    fn test_valid_snapshot_id_different_hex() {
        let result = validate_snapshot_id("snap-abcdef0123456789a");
        assert!(result.is_ok());
    }

    #[test]
    fn test_invalid_snapshot_id_missing_prefix() {
        let result = validate_snapshot_id("0123456789abcdef0");
        assert!(result.is_err());
        assert!(matches!(result, Err(ValidationError::InvalidSnapshotId)));
    }

    #[test]
    fn test_invalid_snapshot_id_wrong_prefix() {
        let result = validate_snapshot_id("vol-0123456789abcdef0");
        assert!(result.is_err());
        assert!(matches!(result, Err(ValidationError::InvalidSnapshotId)));
    }

    #[test]
    fn test_invalid_snapshot_id_empty_string() {
        let result = validate_snapshot_id("");
        assert!(result.is_err());
        assert!(matches!(result, Err(ValidationError::InvalidSnapshotId)));
    }

    #[test]
    fn test_invalid_snapshot_id_only_prefix() {
        let result = validate_snapshot_id("snap-");
        assert!(result.is_err());
        assert!(matches!(result, Err(ValidationError::InvalidSnapshotId)));
    }

    #[test]
    fn test_invalid_snapshot_id_path_traversal() {
        for id in [
            "snap-../../etc/evil",
            "snap-/tmp/x",
            "snap-abc/def",
            "snap-abc\\def",
        ] {
            assert!(
                matches!(
                    validate_snapshot_id(id),
                    Err(ValidationError::InvalidSnapshotId)
                ),
                "{id} should be rejected"
            );
        }
    }

    #[test]
    fn test_invalid_snapshot_id_non_hex_or_uppercase() {
        for id in [
            "snap-ZZZZ",
            "snap-ABCDEF01",
            "snap-0123456789abcdefg",
            "snap- 123",
        ] {
            assert!(
                matches!(
                    validate_snapshot_id(id),
                    Err(ValidationError::InvalidSnapshotId)
                ),
                "{id} should be rejected"
            );
        }
    }

    #[test]
    fn test_invalid_snapshot_id_too_long() {
        let id = format!("snap-{}", "a".repeat(60));
        assert!(validate_snapshot_id(&id).is_err());
        let ok = format!("snap-{}", "a".repeat(59));
        assert!(validate_snapshot_id(&ok).is_ok());
    }

    #[test]
    fn test_parent_args_reject_non_hex_ids() {
        assert!(matches!(
            validate_parent_args(
                &Some("snap-parent".to_string()),
                &Some("snap-00000000000000b02".to_string())
            ),
            Err(ValidationError::InvalidParentSnapshotId)
        ));
    }

    #[test]
    fn test_version_flag_is_supported() {
        let err = CliArgs::try_parse_from(["snapshot-replicator", "--version"]).unwrap_err();
        assert_eq!(err.kind(), clap::error::ErrorKind::DisplayVersion);
    }

    // ====== Error Type Tests ======

    #[test]
    fn test_error_display_invalid_snapshot_id() {
        let error = ValidationError::InvalidSnapshotId;
        let error_message = format!("{}", error);
        assert!(error_message.contains("snapshot ID"));
        assert!(error_message.contains("snap-"));
    }

    #[test]
    fn test_error_display_invalid_region() {
        let error = ValidationError::InvalidRegion;
        let error_message = format!("{}", error);
        assert!(error_message.contains("region"));
        assert!(error_message.contains("us-west-2"));
    }

    #[test]
    fn test_error_debug_implementation() {
        let error = ValidationError::InvalidSnapshotId;
        let debug_output = format!("{:?}", error);
        assert!(debug_output.contains("InvalidSnapshotId"));
    }

    // ====== Integration Tests ======

    #[test]
    fn test_validate_args_success() {
        let args = CliArgs {
            snapshot_id: "snap-0123456789abcdef0".to_string(),
            source_region: "us-east-1".to_string(),
            target_region: "us-west-2".to_string(),
            source_profile: Some("commercial".to_string()),
            target_profile: "govcloud".to_string(),
            concurrency: 100,
            max_attempts: 5,
            encrypt: false,
            kms_key_id: None,
            parent_snapshot_id: None,
            parent_dest_snapshot_id: None,
        };
        let result = validate_args(&args);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_args_invalid_snapshot_id() {
        let args = CliArgs {
            snapshot_id: "vol-123456".to_string(),
            source_region: "us-east-1".to_string(),
            target_region: "us-west-2".to_string(),
            source_profile: None,
            target_profile: "prod".to_string(),
            concurrency: 100,
            max_attempts: 5,
            encrypt: false,
            kms_key_id: None,
            parent_snapshot_id: None,
            parent_dest_snapshot_id: None,
        };
        let result = validate_args(&args);
        assert!(result.is_err());
        assert!(matches!(result, Err(ValidationError::InvalidSnapshotId)));
    }

    #[test]
    fn test_validate_args_invalid_region() {
        let args = CliArgs {
            snapshot_id: "snap-0123456789abcdef0".to_string(),
            source_region: "us-east-1".to_string(),
            target_region: "invalid-region".to_string(),
            source_profile: None,
            target_profile: "prod".to_string(),
            concurrency: 100,
            max_attempts: 5,
            encrypt: false,
            kms_key_id: None,
            parent_snapshot_id: None,
            parent_dest_snapshot_id: None,
        };
        let result = validate_args(&args);
        assert!(result.is_err());
        assert!(matches!(result, Err(ValidationError::InvalidRegion)));
    }

    #[test]
    fn test_validate_args_invalid_source_region() {
        let args = CliArgs {
            snapshot_id: "snap-0123456789abcdef0".to_string(),
            source_region: "not-a-region".to_string(),
            target_region: "us-west-2".to_string(),
            source_profile: None,
            target_profile: "prod".to_string(),
            concurrency: 100,
            max_attempts: 5,
            encrypt: false,
            kms_key_id: None,
            parent_snapshot_id: None,
            parent_dest_snapshot_id: None,
        };
        let result = validate_args(&args);
        assert!(matches!(result, Err(ValidationError::InvalidRegion)));
    }

    #[test]
    fn test_validate_args_multiple_errors_snapshot_first() {
        let args = CliArgs {
            snapshot_id: "invalid".to_string(),
            source_region: "us-east-1".to_string(),
            target_region: "also-invalid".to_string(),
            source_profile: None,
            target_profile: "prod".to_string(),
            concurrency: 100,
            max_attempts: 5,
            encrypt: false,
            kms_key_id: None,
            parent_snapshot_id: None,
            parent_dest_snapshot_id: None,
        };
        let result = validate_args(&args);
        assert!(result.is_err());
        // Should fail on snapshot ID first (? operator short-circuits)
        assert!(matches!(result, Err(ValidationError::InvalidSnapshotId)));
    }

    // ====== KMS Key ARN Validation Tests ======

    #[test]
    fn test_valid_kms_key_arn_commercial() {
        assert!(validate_kms_key_arn(
            "arn:aws:kms:us-west-2:123456789012:key/12345678-1234-1234-1234-123456789012"
        )
        .is_ok());
    }

    #[test]
    fn test_valid_kms_key_arn_govcloud() {
        assert!(validate_kms_key_arn(
            "arn:aws-us-gov:kms:us-gov-west-1:123456789012:key/abcdef01-2345-6789-abcd-ef0123456789"
        )
        .is_ok());
    }

    #[test]
    fn test_valid_kms_key_arn_china() {
        assert!(validate_kms_key_arn(
            "arn:aws-cn:kms:cn-north-1:123456789012:key/abcdef01-2345-6789-abcd-ef0123456789"
        )
        .is_ok());
    }

    #[test]
    fn test_valid_kms_key_arn_eusc() {
        // European Sovereign Cloud uses the aws-eusc partition.
        assert!(validate_kms_key_arn(
            "arn:aws-eusc:kms:eusc-de-east-1:123456789012:key/abcdef01-2345-6789-abcd-ef0123456789"
        )
        .is_ok());
    }

    #[test]
    fn test_invalid_kms_key_arn_unknown_partition() {
        // A partition that merely starts with "aws" must be rejected.
        assert!(matches!(
            validate_kms_key_arn(
                "arn:awsfoo:kms:us-west-2:123456789012:key/12345678-1234-1234-1234-123456789012"
            ),
            Err(ValidationError::InvalidKmsKeyArn)
        ));
    }

    #[test]
    fn test_invalid_kms_key_arn_not_an_arn() {
        assert!(matches!(
            validate_kms_key_arn("not-an-arn"),
            Err(ValidationError::InvalidKmsKeyArn)
        ));
    }

    #[test]
    fn test_invalid_kms_key_arn_wrong_service() {
        assert!(validate_kms_key_arn(
            "arn:aws:s3:us-west-2:123456789012:key/12345678-1234-1234-1234-123456789012"
        )
        .is_err());
    }

    #[test]
    fn test_invalid_kms_key_arn_missing_key_prefix() {
        assert!(validate_kms_key_arn("arn:aws:kms:us-west-2:123456789012:alias/my-key").is_err());
    }

    #[test]
    fn test_invalid_kms_key_arn_empty() {
        assert!(validate_kms_key_arn("").is_err());
    }

    // ====== Encryption CLI Flag Tests ======

    #[test]
    fn test_encrypt_flag_parsed() {
        let result = CliArgs::try_parse_from([
            "program",
            "--snapshot-id",
            "snap-12345",
            "--source-region",
            "us-east-1",
            "--target-region",
            "us-west-2",
            "--target-profile",
            "prod",
            "--encrypt",
        ]);
        assert!(result.is_ok());
        assert!(result.unwrap().encrypt);
    }

    #[test]
    fn test_kms_key_id_parsed() {
        let result = CliArgs::try_parse_from([
            "program",
            "--snapshot-id",
            "snap-12345",
            "--source-region",
            "us-east-1",
            "--target-region",
            "us-west-2",
            "--target-profile",
            "prod",
            "--encrypt",
            "--kms-key-id",
            "arn:aws:kms:us-west-2:123456789012:key/test-key-id",
        ]);
        assert!(result.is_ok());
        let args = result.unwrap();
        assert!(args.encrypt);
        assert_eq!(
            args.kms_key_id,
            Some("arn:aws:kms:us-west-2:123456789012:key/test-key-id".to_string())
        );
    }

    #[test]
    fn test_no_encrypt_flags_defaults() {
        let result = CliArgs::try_parse_from([
            "program",
            "--snapshot-id",
            "snap-12345",
            "--source-region",
            "us-east-1",
            "--target-region",
            "us-west-2",
            "--target-profile",
            "prod",
        ]);
        assert!(result.is_ok());
        let args = result.unwrap();
        assert!(!args.encrypt);
        assert!(args.kms_key_id.is_none());
    }

    #[test]
    fn test_validate_args_invalid_kms_arn() {
        let args = CliArgs {
            snapshot_id: "snap-0123456789abcdef0".to_string(),
            source_region: "us-east-1".to_string(),
            target_region: "us-west-2".to_string(),
            source_profile: None,
            target_profile: "prod".to_string(),
            concurrency: 100,
            max_attempts: 5,
            encrypt: true,
            kms_key_id: Some("not-an-arn".to_string()),
            parent_snapshot_id: None,
            parent_dest_snapshot_id: None,
        };
        assert!(matches!(
            validate_args(&args),
            Err(ValidationError::InvalidKmsKeyArn)
        ));
    }

    #[test]
    fn test_validate_args_valid_kms_arn() {
        let args = CliArgs {
            snapshot_id: "snap-0123456789abcdef0".to_string(),
            source_region: "us-east-1".to_string(),
            target_region: "us-west-2".to_string(),
            source_profile: None,
            target_profile: "prod".to_string(),
            concurrency: 100,
            max_attempts: 5,
            encrypt: true,
            kms_key_id: Some(
                "arn:aws:kms:us-west-2:123456789012:key/12345678-1234-1234-1234-123456789012"
                    .to_string(),
            ),
            parent_snapshot_id: None,
            parent_dest_snapshot_id: None,
        };
        assert!(validate_args(&args).is_ok());
    }

    #[test]
    fn test_error_display_invalid_kms_key_arn() {
        let error = ValidationError::InvalidKmsKeyArn;
        let msg = format!("{}", error);
        assert!(msg.contains("KMS key ARN"));
    }

    // ====== Incremental Parent Args Validation Tests ======

    #[test]
    fn test_parent_args_none_ok() {
        assert!(validate_parent_args(&None, &None).is_ok());
    }

    #[test]
    fn test_parent_args_both_valid_ok() {
        assert!(validate_parent_args(
            &Some("snap-00000000000000a01".to_string()),
            &Some("snap-00000000000000b02".to_string()),
        )
        .is_ok());
    }

    #[test]
    fn test_parent_args_only_source_rejected() {
        assert!(matches!(
            validate_parent_args(&Some("snap-00000000000000a01".to_string()), &None),
            Err(ValidationError::IncompleteParentArgs)
        ));
    }

    #[test]
    fn test_parent_args_only_dest_rejected() {
        assert!(matches!(
            validate_parent_args(&None, &Some("snap-00000000000000b02".to_string())),
            Err(ValidationError::IncompleteParentArgs)
        ));
    }

    #[test]
    fn test_parent_args_bad_format_rejected() {
        assert!(matches!(
            validate_parent_args(
                &Some("vol-notasnapshot".to_string()),
                &Some("snap-00000000000000b02".to_string()),
            ),
            Err(ValidationError::InvalidParentSnapshotId)
        ));
    }

    #[test]
    fn test_encrypt_flags_rejected_with_parent() {
        let mut args = CliArgs {
            snapshot_id: "snap-0123456789abcdef0".to_string(),
            source_region: "us-east-1".to_string(),
            target_region: "us-west-2".to_string(),
            source_profile: None,
            target_profile: "prod".to_string(),
            concurrency: 100,
            max_attempts: 5,
            encrypt: true,
            kms_key_id: None,
            parent_snapshot_id: Some("snap-00000000000000a01".to_string()),
            parent_dest_snapshot_id: Some("snap-00000000000000b02".to_string()),
        };
        assert!(matches!(
            validate_args(&args),
            Err(ValidationError::EncryptionWithParent)
        ));

        args.encrypt = false;
        args.kms_key_id = Some(
            "arn:aws:kms:us-west-2:123456789012:key/12345678-1234-1234-1234-123456789012"
                .to_string(),
        );
        assert!(matches!(
            validate_args(&args),
            Err(ValidationError::EncryptionWithParent)
        ));

        args.kms_key_id = None;
        assert!(validate_args(&args).is_ok());
    }

    #[test]
    fn test_parent_flags_parsed() {
        let result = CliArgs::try_parse_from([
            "program",
            "--snapshot-id",
            "snap-12345",
            "--source-region",
            "us-east-1",
            "--target-region",
            "us-west-2",
            "--target-profile",
            "prod",
            "--parent-snapshot-id",
            "snap-00000000000000a01",
            "--parent-dest-snapshot-id",
            "snap-00000000000000b02",
        ]);
        assert!(result.is_ok());
        let args = result.unwrap();
        assert_eq!(
            args.parent_snapshot_id,
            Some("snap-00000000000000a01".to_string())
        );
        assert_eq!(
            args.parent_dest_snapshot_id,
            Some("snap-00000000000000b02".to_string())
        );
    }
}
