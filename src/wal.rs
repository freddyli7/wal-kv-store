use crate::KVLogError;
use bincode::{Decode, Encode};
use crc32fast::Hasher;
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::time::SystemTime;
use tokio::io::{AsyncReadExt, AsyncSeekExt, SeekFrom};

pub(crate) const MAX_ENTRY_SIZE: usize = 1024 * 1024; // 1 MiB, adjust as needed
pub(crate) const ENTRY_PREFIX_LEN: usize = 4;
pub(crate) const CHECKSUM_LEN: usize = 4;
pub(crate) const WAL_FILE_MAX_SIZE: usize = 1024 * 1024; // 1 MiB, adjust as needed

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

pub(crate) fn is_wal_log_full(path: &Path) -> Result<bool, KVLogError> {
    // TODO
    Ok(true)
}

// next_file_path returns the next file path to write to based on the current file path.
pub(crate) fn next_file_path(current: &str) -> Result<String, KVLogError> {
    let nanos = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    if let Some((static_path, _)) = current.rsplit_once('_') {
        // split the last _
        let new_path = format!("{}_{}", static_path, nanos);
        return Ok(new_path);
    }
    Err(KVLogError::InvalidFilePathFormat {
        msg: "valid file path should be xx_xx".to_string(),
    })
}

// wal_checksum computes the CRC32 checksum of the contents of the WAL file at the given path.
pub(crate) async fn wal_checksum(f: &mut tokio::fs::File) -> Result<u32, KVLogError> {
    // save current position
    let cur = f.seek(SeekFrom::Current(0)).await?;
    f.seek(SeekFrom::Start(0)).await?;

    let mut hasher = Hasher::new();
    let mut buf = [0u8; 8192];
    loop {
        let n = f.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }

    // restore position (append writes will still go to end, but keep it tidy)
    f.seek(SeekFrom::Start(cur)).await?;
    Ok(hasher.finalize())
}
