use crate::KVLogError;
use bincode::{Decode, Encode};
use crc32fast::Hasher;
use serde::{Deserialize, Serialize};

pub(crate) const MAX_ENTRY_SIZE: usize = 1024 * 1024; // 1 MiB, adjust as needed
pub(crate) const ENTRY_PREFIX_LEN: usize = 4;
pub(crate) const CHECKSUM_LEN: usize = 4;

// Rust attribute that auto‑implements traits for a type
#[derive(Serialize, Deserialize, Decode, Encode)]
pub(crate) enum WALEntry<K, V> {
    Set { key: K, value: V },
    Delete { key: K },
}

// crc32 is the checksum algorithm used in Raft
pub(crate) fn crc32(data: &[u8]) -> u32 {
    let mut hasher = Hasher::new();
    hasher.update(data);
    hasher.finalize()
}

// parse_prefix_bytes is a helper function to parse the prefix bytes from the WAL entry payload
pub(crate) fn parse_prefix_bytes(d: &[u8]) -> Result<u32, KVLogError> {
    if d.len() != 4 {
        return Err(KVLogError::InvalidPrefix {
            msg: format!("invalid prefix length: {}", d.len()),
        });
    }
    Ok(u32::from_le_bytes([d[0], d[1], d[2], d[3]]))
}
