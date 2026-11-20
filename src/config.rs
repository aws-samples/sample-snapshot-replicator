// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

//! Replication tuning parameters
//!
//! Concurrency and retry behavior are controlled by CLI flags (`--concurrency`,
//! `--max-attempts`). This module holds the resulting [`ReplicationConfig`] used
//! by the replication engine, plus validation of the user-supplied values.

use thiserror::Error;

/// Configuration validation errors
#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("concurrency must be between 1 and {MAX_CONCURRENCY}")]
    InvalidConcurrency,

    #[error("max-attempts must be between 1 and {MAX_ATTEMPTS}")]
    InvalidAttempts,
}

// Upper bounds to keep user input within sane limits.
const MAX_CONCURRENCY: usize = 10_000;
const MAX_ATTEMPTS: u32 = 20;

/// Configuration for snapshot replication operations
#[derive(Debug, Clone)]
pub struct ReplicationConfig {
    /// Number of concurrent block copy operations
    pub concurrency: usize,
}

impl Default for ReplicationConfig {
    fn default() -> Self {
        Self { concurrency: 100 }
    }
}

impl ReplicationConfig {
    /// Build a configuration from the user-supplied CLI values, validating bounds.
    ///
    /// `max_attempts` is validated here (so an out-of-range value is rejected
    /// once, up front) but not stored: the retry budget is applied by the SDK's
    /// adaptive retryer at client construction in [`crate::auth::AuthManager`],
    /// not by the replication engine.
    pub fn from_cli(concurrency: usize, max_attempts: u32) -> Result<Self, ConfigError> {
        if concurrency == 0 || concurrency > MAX_CONCURRENCY {
            return Err(ConfigError::InvalidConcurrency);
        }
        if max_attempts == 0 || max_attempts > MAX_ATTEMPTS {
            return Err(ConfigError::InvalidAttempts);
        }

        Ok(Self { concurrency })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_defaults() {
        let cfg = ReplicationConfig::default();
        assert_eq!(cfg.concurrency, 100);
    }

    #[test]
    fn test_from_cli_valid() {
        let cfg = ReplicationConfig::from_cli(150, 8).unwrap();
        assert_eq!(cfg.concurrency, 150);
    }

    #[test]
    fn test_from_cli_zero_concurrency_rejected() {
        assert!(matches!(
            ReplicationConfig::from_cli(0, 5),
            Err(ConfigError::InvalidConcurrency)
        ));
    }

    #[test]
    fn test_from_cli_concurrency_too_high_rejected() {
        assert!(matches!(
            ReplicationConfig::from_cli(MAX_CONCURRENCY + 1, 5),
            Err(ConfigError::InvalidConcurrency)
        ));
    }

    #[test]
    fn test_from_cli_zero_attempts_rejected() {
        assert!(matches!(
            ReplicationConfig::from_cli(100, 0),
            Err(ConfigError::InvalidAttempts)
        ));
    }

    #[test]
    fn test_from_cli_too_many_attempts_rejected() {
        assert!(matches!(
            ReplicationConfig::from_cli(100, MAX_ATTEMPTS + 1),
            Err(ConfigError::InvalidAttempts)
        ));
    }
}
