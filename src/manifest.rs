use bincode::{Decode, Encode};
use std::time::SystemTime;

const MANIFEST_FILE_VERSION_1: u8 = 1;

#[derive(Debug, Clone, Encode, Decode)]
pub(crate) struct Manifest {
    _version: u8,
    snapshot_path: String,
    covered_wal_log_path: String,
    _wal_log_checksum: u32,
    timestamp: u64,
}

impl Manifest {
    pub(crate) fn new(
        snapshot_path: String,
        covered_wal_log_path: String,
        _wal_log_checksum: u32,
    ) -> Self {
        Manifest {
            _version: MANIFEST_FILE_VERSION_1,
            snapshot_path,
            covered_wal_log_path,
            _wal_log_checksum,
            timestamp: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        }
    }

    pub(crate) fn get_snapshot_path(&self) -> &str {
        &self.snapshot_path
    }

    pub(crate) fn get_version(&self) -> u8 {
        self._version
    }

    pub(crate) fn get_covered_wal_log_path(&self) -> &str {
        &self.covered_wal_log_path
    }

    pub(crate) fn get_wal_log_checksum(&self) -> u32 {
        self._wal_log_checksum
    }

    pub(crate) fn get_timestamp(&self) -> u64 {
        self.timestamp
    }
}
