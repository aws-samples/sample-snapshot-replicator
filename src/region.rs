// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

//! Region validation for AWS partitions
//!
//! This module provides validation for AWS region formats across different partitions.

use thiserror::Error;

/// Errors that can occur during region validation
#[derive(Error, Debug, PartialEq)]
pub enum RegionError {
    /// Invalid region format
    #[error("Invalid region format: {0}")]
    InvalidFormat(String),
}

/// Validates an AWS region format
///
/// Supports all AWS partitions:
/// - Commercial (aws): us-east-1, eu-west-1, etc.
/// - GovCloud (aws-us-gov): us-gov-west-1, us-gov-east-1
/// - China (aws-cn): cn-north-1, cn-northwest-1
/// - European Sovereign (aws-eusc): eusc-de-east-1
///
/// # Arguments
/// * `region` - The region string to validate
///
/// # Returns
/// * `Ok(())` if the region format is valid
/// * `Err(RegionError)` if the region format is invalid
pub fn validate_region(region: &str) -> Result<(), RegionError> {
    if region.is_empty() {
        return Err(RegionError::InvalidFormat(
            "Region cannot be empty".to_string(),
        ));
    }

    // Check if region matches any known partition pattern
    let is_valid = is_commercial_region(region)
        || is_govcloud_region(region)
        || is_china_region(region)
        || is_eusc_region(region);

    if is_valid {
        Ok(())
    } else {
        Err(RegionError::InvalidFormat(format!(
            "Region '{}' does not match any known AWS partition pattern",
            region
        )))
    }
}

// Helper functions for partition detection
//
// These used to use the `regex` crate, but the four patterns are simple enough
// to match with plain string checks — which drops a dependency and avoids
// recompiling a `Regex` on every call.

/// True if every character is an ASCII lowercase letter (and the slice is non-empty).
fn is_lower_alpha(s: &str) -> bool {
    !s.is_empty() && s.bytes().all(|b| b.is_ascii_lowercase())
}

/// True if every character is an ASCII digit (and the slice is non-empty).
fn is_digits(s: &str) -> bool {
    !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit())
}

/// Matches `^[a-z]{2}-[a-z]+-\d+$` with a 2-letter geo prefix, e.g. `us-east-1`.
///
/// GovCloud (`us-gov-*`), China (`cn-*`), and EUSC (`eusc-*`) regions are handled
/// by their own matchers and excluded here even when they fit the shape.
fn is_commercial_region(region: &str) -> bool {
    if region.starts_with("us-gov-") || region.starts_with("cn-") || region.starts_with("eusc-") {
        return false;
    }
    let parts: Vec<&str> = region.split('-').collect();
    parts.len() == 3
        && parts[0].len() == 2
        && is_lower_alpha(parts[0])
        && is_lower_alpha(parts[1])
        && is_digits(parts[2])
}

/// Matches `^us-gov-[a-z]+-\d+$`, e.g. `us-gov-west-1`.
fn is_govcloud_region(region: &str) -> bool {
    let parts: Vec<&str> = region.split('-').collect();
    parts.len() == 4
        && parts[0] == "us"
        && parts[1] == "gov"
        && is_lower_alpha(parts[2])
        && is_digits(parts[3])
}

/// Matches `^cn-[a-z]+-\d+$`, e.g. `cn-north-1`.
fn is_china_region(region: &str) -> bool {
    let parts: Vec<&str> = region.split('-').collect();
    parts.len() == 3 && parts[0] == "cn" && is_lower_alpha(parts[1]) && is_digits(parts[2])
}

/// Matches `^eusc-[a-z]{2}-[a-z]+-\d+$`, e.g. `eusc-de-east-1`.
fn is_eusc_region(region: &str) -> bool {
    let parts: Vec<&str> = region.split('-').collect();
    parts.len() == 4
        && parts[0] == "eusc"
        && parts[1].len() == 2
        && is_lower_alpha(parts[1])
        && is_lower_alpha(parts[2])
        && is_digits(parts[3])
}

/// Return the AWS partition that a region belongs to.
///
/// Used to build partition-correct ARNs (for example the `SourceSnapshotArn`
/// provenance tag). The mapping mirrors the matchers above:
/// - `us-gov-*` → `aws-us-gov`
/// - `cn-*` → `aws-cn`
/// - `eusc-*` → `aws-eusc`
/// - everything else → `aws`
///
/// This covers the partitions this tool targets; the ISO partitions are not
/// reachable from the others and are not represented here.
pub fn partition_for_region(region: &str) -> &'static str {
    if region.starts_with("us-gov-") {
        "aws-us-gov"
    } else if region.starts_with("cn-") {
        "aws-cn"
    } else if region.starts_with("eusc-") {
        "aws-eusc"
    } else {
        "aws"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_partition_for_commercial_region() {
        assert_eq!(partition_for_region("us-east-1"), "aws");
        assert_eq!(partition_for_region("eu-west-1"), "aws");
    }

    #[test]
    fn test_partition_for_govcloud_region() {
        assert_eq!(partition_for_region("us-gov-west-1"), "aws-us-gov");
    }

    #[test]
    fn test_partition_for_china_region() {
        assert_eq!(partition_for_region("cn-north-1"), "aws-cn");
    }

    #[test]
    fn test_partition_for_eusc_region() {
        assert_eq!(partition_for_region("eusc-de-east-1"), "aws-eusc");
    }

    #[test]
    fn test_valid_commercial_region_us() {
        assert!(validate_region("us-east-1").is_ok());
    }

    #[test]
    fn test_valid_commercial_region_eu() {
        assert!(validate_region("eu-west-1").is_ok());
    }

    #[test]
    fn test_valid_govcloud_region() {
        assert!(validate_region("us-gov-west-1").is_ok());
    }

    #[test]
    fn test_valid_china_region() {
        assert!(validate_region("cn-north-1").is_ok());
    }

    #[test]
    fn test_valid_eusc_region() {
        assert!(validate_region("eusc-de-east-1").is_ok());
    }

    #[test]
    fn test_invalid_region_format() {
        assert!(validate_region("invalid-region").is_err());
    }

    #[test]
    fn test_invalid_region_incomplete() {
        assert!(validate_region("us-east").is_err());
    }

    #[test]
    fn test_empty_region() {
        assert!(validate_region("").is_err());
    }
}
