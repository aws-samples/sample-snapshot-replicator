// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

//! Progress reporting for snapshot replication
//!
//! Provides real-time progress display with throughput and ETA calculations
//! for concurrent block copying operations.

use crate::block_copier::EBS_BLOCK_SIZE;
use indicatif::{ProgressBar, ProgressStyle};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

/// Reports replication progress with real-time updates
///
/// Thread-safe progress reporter using atomic counters for concurrent updates
/// from multiple Tokio tasks. Displays progress bar with percentage, blocks
/// copied, throughput, and estimated time to completion.
pub struct ProgressReporter {
    total_blocks: usize,
    initial_position: usize,
    completed_blocks: AtomicUsize,
    start_time: Instant,
    progress_bar: ProgressBar,
}

impl ProgressReporter {
    /// Creates a new progress reporter
    ///
    /// # Arguments
    /// * `total_blocks` - Total number of blocks to copy
    /// * `initial_position` - Number of blocks already completed (for resume)
    pub fn new(total_blocks: usize, initial_position: usize) -> Self {
        let progress_bar = ProgressBar::new(total_blocks as u64);
        progress_bar.set_style(
            ProgressStyle::default_bar()
                .template("[{bar:40}] {percent}% ({pos}/{len} blocks) - {msg}")
                .unwrap()
                .progress_chars("█░"),
        );
        progress_bar.set_position(initial_position as u64);

        Self {
            total_blocks,
            initial_position,
            completed_blocks: AtomicUsize::new(initial_position),
            start_time: Instant::now(),
            progress_bar,
        }
    }

    /// Updates progress after a block is copied
    ///
    /// Thread-safe method that increments the completed block counter
    /// and refreshes the progress display.
    pub fn update(&self) {
        let completed = self.completed_blocks.fetch_add(1, Ordering::SeqCst) + 1;
        self.progress_bar.set_position(completed as u64);

        let throughput = self.throughput();
        let msg = if let Some(eta) = self.eta() {
            format!("{:.1} MB/s - ETA: {:.0}s", throughput, eta)
        } else {
            format!("{:.1} MB/s", throughput)
        };
        self.progress_bar.set_message(msg);
    }

    /// Completes the progress bar and displays final summary
    pub fn finish(&self) {
        self.progress_bar.finish_with_message("Complete");
    }

    /// Gets current completed block count
    fn completed(&self) -> usize {
        self.completed_blocks.load(Ordering::SeqCst)
    }

    /// Calculates throughput in MB/s (only counts blocks copied in this run)
    fn throughput(&self) -> f64 {
        let elapsed = self.start_time.elapsed().as_secs_f64();
        if elapsed == 0.0 {
            return 0.0;
        }
        let new_blocks = self.completed().saturating_sub(self.initial_position) as f64;
        let bytes = new_blocks * EBS_BLOCK_SIZE as f64;
        bytes / elapsed / 1024.0 / 1024.0
    }

    /// Calculates estimated time remaining in seconds
    fn eta(&self) -> Option<f64> {
        let new_blocks = self.completed().saturating_sub(self.initial_position);
        if new_blocks == 0 {
            return None;
        }
        let elapsed = self.start_time.elapsed().as_secs_f64();
        let remaining = self.total_blocks.saturating_sub(self.completed());
        Some((remaining as f64 * elapsed) / new_blocks as f64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_progress_reporter_creation() {
        let reporter = ProgressReporter::new(100, 0);
        assert_eq!(reporter.total_blocks, 100);
        assert_eq!(reporter.completed(), 0);
    }

    #[test]
    fn test_progress_reporter_with_initial_position() {
        let reporter = ProgressReporter::new(100, 40);
        assert_eq!(reporter.total_blocks, 100);
        assert_eq!(reporter.completed(), 40);
    }

    #[test]
    fn test_single_update() {
        let reporter = ProgressReporter::new(100, 0);
        reporter.update();
        assert_eq!(reporter.completed(), 1);
    }

    #[test]
    fn test_multiple_updates() {
        let reporter = ProgressReporter::new(100, 0);
        for _ in 0..50 {
            reporter.update();
        }
        assert_eq!(reporter.completed(), 50);
    }

    #[test]
    fn test_throughput_calculation() {
        // Simulate 100 blocks copied in 10 seconds
        let _reporter = ProgressReporter::new(100, 0);

        // Manually set start time to 10 seconds ago
        let past_time = Instant::now() - Duration::from_secs(10);
        let reporter = ProgressReporter {
            total_blocks: 100,
            initial_position: 0,
            completed_blocks: AtomicUsize::new(100),
            start_time: past_time,
            progress_bar: ProgressBar::hidden(),
        };

        let throughput = reporter.throughput();
        // 100 blocks * 512 KiB / 10 seconds = 5.0 MB/s
        assert!(
            (throughput - 5.0).abs() < 0.1,
            "Expected ~5.0 MB/s, got {}",
            throughput
        );
    }

    #[test]
    fn test_throughput_zero_elapsed() {
        let reporter = ProgressReporter::new(100, 0);
        // Immediately check throughput (elapsed ~0)
        let throughput = reporter.throughput();
        assert_eq!(throughput, 0.0);
    }

    #[test]
    fn test_eta_calculation() {
        // Simulate 50 of 100 blocks in 10 seconds
        let past_time = Instant::now() - Duration::from_secs(10);
        let reporter = ProgressReporter {
            total_blocks: 100,
            initial_position: 0,
            completed_blocks: AtomicUsize::new(50),
            start_time: past_time,
            progress_bar: ProgressBar::hidden(),
        };

        let eta = reporter.eta().expect("ETA should be Some");
        // 50 remaining * 10 seconds / 50 completed = 10 seconds
        assert!((eta - 10.0).abs() < 0.1, "Expected ~10.0s ETA, got {}", eta);
    }

    #[test]
    fn test_eta_zero_completed() {
        let reporter = ProgressReporter::new(100, 0);
        let eta = reporter.eta();
        assert!(eta.is_none(), "ETA should be None when no blocks completed");
    }

    #[test]
    fn test_eta_all_completed() {
        // All blocks completed
        let past_time = Instant::now() - Duration::from_secs(10);
        let reporter = ProgressReporter {
            total_blocks: 100,
            initial_position: 0,
            completed_blocks: AtomicUsize::new(100),
            start_time: past_time,
            progress_bar: ProgressBar::hidden(),
        };

        let eta = reporter.eta().expect("ETA should be Some");
        assert_eq!(eta, 0.0, "ETA should be 0 when all blocks completed");
    }

    #[test]
    fn test_concurrent_updates() {
        let reporter = Arc::new(ProgressReporter::new(1000, 0));
        let mut handles = vec![];

        // Spawn 10 threads, each updating 100 times
        for _ in 0..10 {
            let reporter_clone = Arc::clone(&reporter);
            let handle = thread::spawn(move || {
                for _ in 0..100 {
                    reporter_clone.update();
                }
            });
            handles.push(handle);
        }

        // Wait for all threads
        for handle in handles {
            handle.join().unwrap();
        }

        // Verify all 1000 updates were counted
        assert_eq!(reporter.completed(), 1000);
    }

    #[test]
    fn test_finish_method() {
        let reporter = ProgressReporter::new(10, 0);
        for _ in 0..10 {
            reporter.update();
        }
        reporter.finish();
        // Should not panic
        assert_eq!(reporter.completed(), 10);
    }
}
