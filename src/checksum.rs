// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

//! Checksum calculation for snapshot validation
//!
//! This module computes the aggregate checksum used in the `CompleteSnapshot`
//! API call to validate data integrity.

use crate::BlockChecksum;
use base64::{engine::general_purpose, Engine as _};
use sha2::{Digest, Sha256};
use thiserror::Error;

/// Error returned when an aggregate checksum cannot be computed.
#[derive(Debug, Error)]
pub enum ChecksumError {
    /// A block's stored checksum was not valid base64 and could not be decoded.
    ///
    /// This typically means a corrupt or truncated resume state file (for
    /// example a line torn by an interrupted write). The offending block should
    /// be re-copied rather than skipped, otherwise the aggregate would silently
    /// cover fewer blocks than were actually written and `CompleteSnapshot`
    /// would reject it.
    #[error("block {index} has an invalid base64 checksum")]
    InvalidChecksum { index: i32 },
}

/// Calculate the aggregate checksum from block checksums using the LINEAR method.
///
/// This implements the LINEAR aggregation algorithm required by the EBS
/// `CompleteSnapshot` API:
/// 1. Sort block checksums by index (ascending)
/// 2. Decode each base64 checksum to raw bytes
/// 3. Concatenate all raw checksum bytes
/// 4. Calculate SHA256 hash of concatenated bytes
/// 5. Base64-encode the hash
///
/// # Arguments
///
/// * `block_checksums` - Slice of [`BlockChecksum`] with index and base64-encoded checksum
///
/// # Returns
///
/// Base64-encoded SHA256 hash of concatenated raw checksums.
///
/// # Errors
///
/// Returns [`ChecksumError::InvalidChecksum`] if any block's checksum is not
/// valid base64. Failing here is deliberate: silently skipping an undecodable
/// checksum would compute the aggregate over fewer blocks than were written,
/// and the mismatch would only surface as an opaque `CompleteSnapshot` failure.
pub fn aggregate_checksum(block_checksums: &[BlockChecksum]) -> Result<String, ChecksumError> {
    // Sort by block index
    let mut sorted: Vec<_> = block_checksums.to_vec();
    sorted.sort_by_key(|b| b.index);

    // Decode base64 checksums and concatenate raw bytes. An undecodable
    // checksum is a hard error rather than a silent skip (see `# Errors`).
    let mut concatenated_bytes = Vec::new();
    for block in &sorted {
        let decoded = general_purpose::STANDARD
            .decode(&block.checksum)
            .map_err(|_| ChecksumError::InvalidChecksum { index: block.index })?;
        concatenated_bytes.extend_from_slice(&decoded);
    }

    // Calculate SHA256 hash of concatenated bytes
    let mut hasher = Sha256::new();
    hasher.update(&concatenated_bytes);
    let hash = hasher.finalize();

    // Base64-encode
    Ok(general_purpose::STANDARD.encode(hash))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_known_test_vector() {
        // Known test vector with expected output
        // Create base64-encoded SHA256 checksums for test data
        let mut hasher0 = Sha256::new();
        hasher0.update(b"block0");
        let checksum0 = general_purpose::STANDARD.encode(hasher0.finalize());

        let mut hasher1 = Sha256::new();
        hasher1.update(b"block1");
        let checksum1 = general_purpose::STANDARD.encode(hasher1.finalize());

        let mut hasher2 = Sha256::new();
        hasher2.update(b"block2");
        let checksum2 = general_purpose::STANDARD.encode(hasher2.finalize());

        let checksums = vec![
            BlockChecksum {
                index: 0,
                checksum: checksum0.clone(),
            },
            BlockChecksum {
                index: 1,
                checksum: checksum1.clone(),
            },
            BlockChecksum {
                index: 2,
                checksum: checksum2.clone(),
            },
        ];

        let result = aggregate_checksum(&checksums).unwrap();
        let bytes0 = general_purpose::STANDARD.decode(&checksum0).unwrap();
        let bytes1 = general_purpose::STANDARD.decode(&checksum1).unwrap();
        let bytes2 = general_purpose::STANDARD.decode(&checksum2).unwrap();

        let mut concatenated = Vec::new();
        concatenated.extend_from_slice(&bytes0);
        concatenated.extend_from_slice(&bytes1);
        concatenated.extend_from_slice(&bytes2);

        let mut hasher = Sha256::new();
        hasher.update(&concatenated);
        let expected = general_purpose::STANDARD.encode(hasher.finalize());

        assert_eq!(result, expected);
    }

    #[test]
    fn test_sorting_behavior() {
        // Verify checksums are sorted by index before concatenation
        let checksums_unsorted = vec![
            BlockChecksum {
                index: 2,
                checksum: "Z3Vp".to_string(),
            },
            BlockChecksum {
                index: 0,
                checksum: "YWJj".to_string(),
            },
            BlockChecksum {
                index: 1,
                checksum: "ZGVm".to_string(),
            },
        ];

        let checksums_sorted = vec![
            BlockChecksum {
                index: 0,
                checksum: "YWJj".to_string(),
            },
            BlockChecksum {
                index: 1,
                checksum: "ZGVm".to_string(),
            },
            BlockChecksum {
                index: 2,
                checksum: "Z3Vp".to_string(),
            },
        ];

        let result_unsorted = aggregate_checksum(&checksums_unsorted).unwrap();
        let result_sorted = aggregate_checksum(&checksums_sorted).unwrap();

        // Both should produce the same result after sorting
        assert_eq!(result_unsorted, result_sorted);
    }

    #[test]
    fn test_deterministic_output() {
        // Same input produces same output
        let checksums = vec![
            BlockChecksum {
                index: 0,
                checksum: "dGVzdDE=".to_string(),
            },
            BlockChecksum {
                index: 1,
                checksum: "dGVzdDI=".to_string(),
            },
        ];

        let result1 = aggregate_checksum(&checksums).unwrap();
        let result2 = aggregate_checksum(&checksums).unwrap();
        let result3 = aggregate_checksum(&checksums).unwrap();

        assert_eq!(result1, result2);
        assert_eq!(result2, result3);
    }

    #[test]
    fn test_empty_input() {
        // Empty input should return hash of empty string
        let checksums: Vec<BlockChecksum> = vec![];

        let result = aggregate_checksum(&checksums).unwrap();

        // Calculate expected: SHA256("") base64-encoded
        let mut hasher = Sha256::new();
        hasher.update(b"");
        let expected = general_purpose::STANDARD.encode(hasher.finalize());

        assert_eq!(result, expected);
    }

    #[test]
    fn test_single_block() {
        // Single block should work correctly
        // Create base64-encoded SHA256 checksum for test data
        let mut hasher0 = Sha256::new();
        hasher0.update(b"test data");
        let checksum0 = general_purpose::STANDARD.encode(hasher0.finalize());

        let checksums = vec![BlockChecksum {
            index: 0,
            checksum: checksum0.clone(),
        }];

        let result = aggregate_checksum(&checksums);

        // Calculate expected: decode, hash, encode
        let bytes0 = general_purpose::STANDARD.decode(&checksum0).unwrap();
        let mut hasher = Sha256::new();
        hasher.update(&bytes0);
        let expected = general_purpose::STANDARD.encode(hasher.finalize());

        assert_eq!(result.unwrap(), expected);
    }

    #[test]
    fn test_large_input() {
        // Large input (100 blocks) should work correctly
        let checksums: Vec<BlockChecksum> = (0..100)
            .map(|i| {
                // Use a real base64-encoded SHA256 digest per block.
                let mut hasher = Sha256::new();
                hasher.update(format!("block{i}").as_bytes());
                BlockChecksum {
                    index: i,
                    checksum: general_purpose::STANDARD.encode(hasher.finalize()),
                }
            })
            .collect();

        let result = aggregate_checksum(&checksums).unwrap();

        // Verify result is valid base64
        assert!(!result.is_empty());
        assert!(general_purpose::STANDARD.decode(&result).is_ok());

        // Verify determinism with large input
        let result2 = aggregate_checksum(&checksums).unwrap();
        assert_eq!(result, result2);
    }

    #[test]
    fn test_invalid_base64_checksum_is_error() {
        // A corrupt/truncated checksum (e.g. a torn resume-state line) must be
        // surfaced, not silently skipped.
        let checksums = vec![
            BlockChecksum {
                index: 0,
                checksum: "dGVzdDE=".to_string(), // valid base64
            },
            BlockChecksum {
                index: 1,
                checksum: "not valid base64!!".to_string(),
            },
        ];

        let err = aggregate_checksum(&checksums).unwrap_err();
        assert!(matches!(err, ChecksumError::InvalidChecksum { index: 1 }));
    }
}
