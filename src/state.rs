// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

//! State file persistence for resume capability
//!
//! Tracks completed block copies in an append-only CSV file so interrupted
//! replications can skip already-copied blocks on restart.

use crate::block_copier::BlockChecksum;
use base64::{engine::general_purpose, Engine as _};
use std::collections::{HashMap, HashSet};
use std::io::{self, BufRead, Write};
use std::path::PathBuf;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

/// State loaded from an existing state file on resume
pub struct ResumedState {
    /// Block indexes that were successfully copied
    pub completed_indexes: HashSet<i32>,
    /// Checksums for completed blocks (needed for aggregate checksum)
    pub checksums: Vec<BlockChecksum>,
}

/// Append-only state file for tracking completed block copies
///
/// Uses a dedicated writer task to serialize all disk I/O, avoiding lock
/// contention between the 100+ concurrent copy tasks.
pub struct StateFile {
    tx: mpsc::UnboundedSender<BlockChecksum>,
    writer_handle: JoinHandle<()>,
}

fn path_for(dest_snapshot_id: &str) -> PathBuf {
    PathBuf::from(format!(
        "./snapshot-replicator-state-{}.csv",
        dest_snapshot_id
    ))
}

impl StateFile {
    /// Create a new state file and spawn the writer task
    pub fn new(dest_snapshot_id: &str) -> io::Result<Self> {
        let path = path_for(dest_snapshot_id);
        let mut options = std::fs::OpenOptions::new();
        options.create(true).append(true);
        // The state file lists block checksums for the replica; keep it private
        // to the current user rather than inheriting a permissive umask.
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let file = options.open(&path)?;

        let (tx, mut rx) = mpsc::unbounded_channel::<BlockChecksum>();

        let writer_handle = tokio::spawn(async move {
            let mut file = file;
            while let Some(bc) = rx.recv().await {
                if let Err(e) = writeln!(file, "{},{}", bc.index, bc.checksum) {
                    log::warn!("Failed to write state entry: {}", e);
                }
            }
        });

        Ok(Self { tx, writer_handle })
    }

    /// Load existing state from a previous interrupted run.
    /// Returns `Ok(None)` if no state file exists.
    pub fn load(dest_snapshot_id: &str) -> io::Result<Option<ResumedState>> {
        let path = path_for(dest_snapshot_id);
        if !path.exists() {
            return Ok(None);
        }

        let file = std::fs::File::open(&path)?;
        let reader = io::BufReader::new(file);

        // Use HashMap so duplicate indexes take last value
        let mut map: HashMap<i32, String> = HashMap::new();

        for line in reader.lines() {
            let line = line?;
            if line.is_empty() {
                continue;
            }
            match line.split_once(',') {
                Some((idx_str, checksum)) if !checksum.is_empty() => match idx_str.parse::<i32>() {
                    Ok(idx) => {
                        // Keep a line only if its checksum decodes to a 32-byte
                        // SHA-256 digest. A torn or corrupt line (e.g. a write
                        // interrupted mid-flush) is dropped so that block is
                        // re-copied on the next run, rather than being marked
                        // completed and then failing `CompleteSnapshot` forever
                        // because `aggregate_checksum` rejects the bad value.
                        match general_purpose::STANDARD.decode(checksum) {
                            Ok(bytes) if bytes.len() == 32 => {
                                map.insert(idx, checksum.to_string());
                            }
                            _ => {
                                log::warn!(
                                    "Dropping state line with invalid checksum (will re-copy block {idx}): {line}"
                                );
                            }
                        }
                    }
                    Err(_) => {
                        log::warn!("Skipping malformed state line (bad index): {}", line);
                    }
                },
                _ => {
                    log::warn!("Skipping malformed state line: {}", line);
                }
            }
        }

        let completed_indexes: HashSet<i32> = map.keys().copied().collect();
        let checksums: Vec<BlockChecksum> = map
            .into_iter()
            .map(|(index, checksum)| BlockChecksum { index, checksum })
            .collect();

        Ok(Some(ResumedState {
            completed_indexes,
            checksums,
        }))
    }

    /// Record a completed block (non-blocking send to writer task)
    pub fn record(&self, checksum: BlockChecksum) {
        // Ignore send errors — writer task may have exited on disk full
        let _ = self.tx.send(checksum);
    }

    /// Flush and close the writer task
    pub async fn close(self) {
        drop(self.tx);
        let _ = self.writer_handle.await;
    }

    /// Delete the state file (call after successful completion)
    pub fn cleanup(dest_snapshot_id: &str) -> io::Result<()> {
        let path = path_for(dest_snapshot_id);
        if path.exists() {
            std::fs::remove_file(path)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn unique_id() -> String {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::time::{SystemTime, UNIX_EPOCH};
        // Tests run in parallel and the clock alone is too coarse to tell them
        // apart, so a process-wide counter guarantees distinct state files.
        static COUNTER: AtomicUsize = AtomicUsize::new(0);
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .subsec_nanos();
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        format!("test-{}-{}-{}", std::process::id(), nanos, n)
    }

    /// A valid base64-encoded 32-byte SHA-256 digest derived from `seed`.
    /// `StateFile::load` now keeps only checksums that decode to 32 bytes, so
    /// tests must use real digests rather than arbitrary text.
    fn valid_checksum(seed: &str) -> String {
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        hasher.update(seed.as_bytes());
        general_purpose::STANDARD.encode(hasher.finalize())
    }

    #[tokio::test]
    async fn test_write_and_load_round_trip() {
        let id = unique_id();
        let state = StateFile::new(&id).unwrap();

        state.record(BlockChecksum {
            index: 0,
            checksum: valid_checksum("checksum0"),
        });
        state.record(BlockChecksum {
            index: 5,
            checksum: valid_checksum("checksum5"),
        });
        state.record(BlockChecksum {
            index: 2,
            checksum: valid_checksum("checksum2"),
        });

        state.close().await;

        let resumed = StateFile::load(&id).unwrap().unwrap();
        assert_eq!(resumed.completed_indexes.len(), 3);
        assert!(resumed.completed_indexes.contains(&0));
        assert!(resumed.completed_indexes.contains(&2));
        assert!(resumed.completed_indexes.contains(&5));
        assert_eq!(resumed.checksums.len(), 3);

        StateFile::cleanup(&id).unwrap();
    }

    #[tokio::test]
    async fn test_malformed_line_handling() {
        let id = unique_id();
        let path = path_for(&id);
        let c0 = valid_checksum("0");
        let c3 = valid_checksum("3");

        // Write a file with mixed valid/invalid lines directly
        {
            let mut f = std::fs::File::create(&path).unwrap();
            writeln!(f, "0,{c0}").unwrap();
            writeln!(f).unwrap(); // empty line
            writeln!(f, "no_comma_here").unwrap();
            writeln!(f, ",missing_index").unwrap();
            writeln!(f, "3,{c3}").unwrap();
        }

        let resumed = StateFile::load(&id).unwrap().unwrap();
        assert_eq!(resumed.completed_indexes.len(), 2);
        assert!(resumed.completed_indexes.contains(&0));
        assert!(resumed.completed_indexes.contains(&3));

        StateFile::cleanup(&id).unwrap();
    }

    #[tokio::test]
    async fn test_invalid_checksum_lines_are_dropped() {
        // A torn/corrupt checksum must be dropped on load so the block is
        // re-copied next run, instead of being marked completed and then
        // failing CompleteSnapshot forever.
        let id = unique_id();
        let path = path_for(&id);
        let good = valid_checksum("good");

        {
            let mut f = std::fs::File::create(&path).unwrap();
            // Valid 32-byte digest: kept.
            writeln!(f, "0,{good}").unwrap();
            // Not valid base64: dropped.
            writeln!(f, "1,not valid base64!!").unwrap();
            // Valid base64 but wrong length (not 32 bytes): dropped.
            writeln!(f, "2,dGVzdA==").unwrap();
        }

        let resumed = StateFile::load(&id).unwrap().unwrap();
        assert_eq!(resumed.completed_indexes.len(), 1);
        assert!(resumed.completed_indexes.contains(&0));
        assert!(!resumed.completed_indexes.contains(&1));
        assert!(!resumed.completed_indexes.contains(&2));

        StateFile::cleanup(&id).unwrap();
    }

    #[tokio::test]
    async fn test_duplicate_index_last_value_wins() {
        let id = unique_id();
        let path = path_for(&id);
        let first = valid_checksum("first");
        let second = valid_checksum("second");
        let third = valid_checksum("third");

        {
            let mut f = std::fs::File::create(&path).unwrap();
            writeln!(f, "0,{first}").unwrap();
            writeln!(f, "0,{second}").unwrap();
            writeln!(f, "0,{third}").unwrap();
        }

        let resumed = StateFile::load(&id).unwrap().unwrap();
        assert_eq!(resumed.completed_indexes.len(), 1);
        assert!(resumed.completed_indexes.contains(&0));

        let checksum = resumed.checksums.iter().find(|c| c.index == 0).unwrap();
        assert_eq!(checksum.checksum, third);

        StateFile::cleanup(&id).unwrap();
    }

    #[test]
    fn test_missing_file_returns_none() {
        let result = StateFile::load("snap-does-not-exist-xyz").unwrap();
        assert!(result.is_none());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn test_new_state_file_is_owner_only() {
        use std::os::unix::fs::PermissionsExt;

        let id = unique_id();
        let state = StateFile::new(&id).unwrap();
        state.close().await;

        let mode = std::fs::metadata(path_for(&id))
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o600, "state file must be created 0600");

        StateFile::cleanup(&id).unwrap();
    }

    #[tokio::test]
    async fn test_cleanup_deletes_file() {
        let id = unique_id();
        let state = StateFile::new(&id).unwrap();
        state.close().await;

        let path = path_for(&id);
        assert!(path.exists());

        StateFile::cleanup(&id).unwrap();
        assert!(!path.exists());
    }
}
