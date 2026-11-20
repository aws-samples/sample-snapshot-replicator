// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

//! Authentication management for AWS clients
//!
//! This module manages AWS client creation for source and destination partitions,
//! supporting separate AWS profiles for cross-partition authentication.

use aws_config::profile::ProfileFileCredentialsProvider;
use aws_config::retry::RetryConfig;
use aws_config::{BehaviorVersion, Region, SdkConfig};

/// Manages authentication for AWS clients across different partitions
///
/// The AuthManager holds AWS profile names for creating authenticated clients
/// in both source and destination partitions. Since AWS credentials cannot be
/// shared across partitions, separate profiles are required for each.
#[derive(Debug, Clone)]
pub struct AuthManager {
    /// AWS profile name for the source partition (optional)
    source_profile: Option<String>,

    /// AWS profile name for the target partition (required)
    target_profile: String,

    /// Maximum attempts for the SDK's adaptive retryer (first try + retries)
    max_attempts: u32,
}

impl AuthManager {
    /// Creates a new `AuthManager` with the specified profile names
    ///
    /// # Arguments
    ///
    /// * `source_profile` - Optional AWS profile for the source partition
    /// * `target_profile` - AWS profile for the target partition
    /// * `max_attempts` - Total attempts per request for the SDK adaptive retryer
    ///   (`1` disables retries; must be `>= 1`)
    pub fn new(source_profile: Option<String>, target_profile: String, max_attempts: u32) -> Self {
        Self {
            source_profile,
            target_profile,
            max_attempts,
        }
    }

    /// Build a base [`SdkConfig`] for a given profile and region.
    ///
    /// Credential precedence: when a `profile` is named, an explicit
    /// [`ProfileFileCredentialsProvider`] is installed so that ambient
    /// `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` environment variables cannot
    /// silently override the requested profile. This matters for cross-partition
    /// replication, where the source side is commonly configured through
    /// environment credentials: without the explicit provider those credentials
    /// would win over `--target-profile` and the replica could land in the wrong
    /// account. When no profile is named the standard default credential chain is
    /// used.
    ///
    /// Retries are delegated entirely to the SDK's adaptive retryer (which
    /// includes a client-side rate limiter for throttling), bounded by
    /// `max_attempts`.
    async fn build_sdk_config(&self, profile: Option<&str>, region: &str) -> SdkConfig {
        let retry_config = RetryConfig::adaptive().with_max_attempts(self.max_attempts);

        let loader = aws_config::defaults(BehaviorVersion::latest())
            .region(Region::new(region.to_string()))
            .retry_config(retry_config);

        let loader = match profile {
            Some(profile) => {
                let creds = ProfileFileCredentialsProvider::builder()
                    .profile_name(profile)
                    .build();
                // Also set profile_name so profile-sourced region/settings resolve
                // from the same profile, and install the explicit credentials
                // provider so the environment provider cannot take precedence.
                loader.profile_name(profile).credentials_provider(creds)
            }
            None => loader,
        };

        loader.load().await
    }

    /// Build an [`SdkConfig`] for the source partition (profile optional).
    ///
    /// Exposed so callers that need other clients in the same partition (for
    /// example EC2 for metadata) reuse identical credential and retry wiring.
    pub async fn source_sdk_config(&self, region: &str) -> SdkConfig {
        self.build_sdk_config(self.source_profile.as_deref(), region)
            .await
    }

    /// Build an [`SdkConfig`] for the target partition.
    pub async fn target_sdk_config(&self, region: &str) -> SdkConfig {
        self.build_sdk_config(Some(&self.target_profile), region)
            .await
    }

    /// Creates an authenticated AWS EBS client for the source partition.
    pub async fn create_source_client(&self, region: &str) -> aws_sdk_ebs::Client {
        let config = self.source_sdk_config(region).await;
        aws_sdk_ebs::Client::new(&config)
    }

    /// Creates an authenticated AWS EBS client for the target partition.
    pub async fn create_target_client(&self, region: &str) -> aws_sdk_ebs::Client {
        let config = self.target_sdk_config(region).await;
        aws_sdk_ebs::Client::new(&config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_auth_manager_with_both_profiles() {
        let manager = AuthManager::new(
            Some("source-profile".to_string()),
            "target-profile".to_string(),
            5,
        );
        assert_eq!(manager.source_profile, Some("source-profile".to_string()));
        assert_eq!(manager.target_profile, "target-profile");
    }

    #[test]
    fn test_create_auth_manager_with_target_only() {
        let manager = AuthManager::new(None, "target-profile".to_string(), 5);
        assert_eq!(manager.source_profile, None);
        assert_eq!(manager.target_profile, "target-profile");
    }

    #[test]
    fn test_create_auth_manager_different_names() {
        let manager1 = AuthManager::new(Some("prod".to_string()), "dev".to_string(), 5);
        let manager2 = AuthManager::new(Some("staging".to_string()), "production".to_string(), 5);

        assert_eq!(manager1.source_profile, Some("prod".to_string()));
        assert_eq!(manager1.target_profile, "dev");
        assert_eq!(manager2.source_profile, Some("staging".to_string()));
        assert_eq!(manager2.target_profile, "production");
    }

    #[test]
    fn test_auth_manager_field_access() {
        let manager = AuthManager::new(Some("my-source".to_string()), "my-target".to_string(), 5);
        let source = &manager.source_profile;
        let target = &manager.target_profile;

        assert_eq!(source, &Some("my-source".to_string()));
        assert_eq!(target, "my-target");
    }

    #[test]
    fn test_auth_manager_clone() {
        let manager = AuthManager::new(Some("source".to_string()), "target".to_string(), 5);
        let cloned = manager.clone();

        assert_eq!(manager.source_profile, cloned.source_profile);
        assert_eq!(manager.target_profile, cloned.target_profile);
    }

    #[test]
    fn test_auth_manager_debug() {
        let manager = AuthManager::new(Some("source".to_string()), "target".to_string(), 5);
        let debug_str = format!("{:?}", manager);

        assert!(debug_str.contains("AuthManager"));
        assert!(debug_str.contains("source_profile"));
        assert!(debug_str.contains("target_profile"));
    }
}
