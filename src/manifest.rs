use bincode::{Decode, Encode};
use std::time::SystemTime;

const MANIFEST_FILE_VERSION_1: u8 = 1;

#[derive(Debug, Clone, Encode, Decode)]
pub(crate) struct Manifest {
    version: u8,
    snapshot: String,
    covered_wal_log: String,
    wal_log_checksum: u32,
    timestamp: u64,
}

impl Manifest {
    pub(crate) fn new(snapshot: String, covered_wal_log: String, wal_log_checksum: u32) -> Self {
        Manifest {
            version: MANIFEST_FILE_VERSION_1,
            snapshot,
            covered_wal_log,
            wal_log_checksum,
            timestamp: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        }
    }
}
