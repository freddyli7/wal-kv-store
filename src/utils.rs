use crate::KVLogError;
use bincode::{Decode, Encode};
use crc32fast::Hasher;
use serde::{Deserialize, Serialize};
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::fs::OpenOptions;
use tokio::io::{AsyncReadExt, AsyncSeekExt, SeekFrom};
use tokio::sync::RwLock;

pub(crate) const ANTI_MULTI_PROCESS_LOCK_FILE_NAME: &str = "kvlog.lock";
pub(crate) const MAX_ENTRY_SIZE: usize = 1024 * 1024; // 1 MiB, adjust as needed
pub(crate) const ENTRY_PREFIX_LEN: usize = 4;
pub(crate) const CHECKSUM_LEN: usize = 4;
pub(crate) const WAL_FILE_MAX_SIZE: usize = 1024 * 1024; // 1 MiB, adjust as needed
pub(crate) const DEFAULT_WAL_PATH: &str = "wal_0";
pub(crate) const DEFAULT_SNAPSHOT_PATH: &str = "snapshot_0";

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

pub(crate) fn is_wal_log_full(current_size: usize) -> bool {
    if current_size > WAL_FILE_MAX_SIZE {
        return true;
    }
    false
}

// next_file_path returns the next file path to write to based on the current file path.
pub(crate) fn next_file_path(current: &str) -> Result<String, KVLogError> {
    // split the last _
    if let Some((static_path, num_str)) = current.rsplit_once('_') {
        let number = num_str.parse::<u64>()?;
        let new_path = format!("{}_{}", static_path, number + 1);
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

pub(crate) async fn load_file_as_bytes_in_full(path: &str) -> Result<Option<Vec<u8>>, KVLogError> {
    let file_handle = match OpenOptions::new().read(true).open(path).await {
        Ok(file) => Some(file),
        Err(e) if e.kind() == ErrorKind::NotFound => None,
        Err(e) => {
            return Err(KVLogError::LoadingError {
                msg: format!("failed to load file from path: {}, error: {}", path, e),
            });
        }
    };

    if let Some(file) = file_handle {
        let a_f = Arc::new(RwLock::new(file));
        let mut write_lock = a_f.write().await;
        write_lock.seek(SeekFrom::Start(0)).await?;

        let mut buf = Vec::new();
        write_lock.read_to_end(&mut buf).await?;

        Ok(Some(buf))
    } else {
        Ok(None)
    }
}

pub(crate) fn get_parent_dir(path: &str) -> Result<PathBuf, KVLogError> {
    Path::new(path)
        .parent()
        .map(|p| p.to_path_buf())
        .ok_or(KVLogError::InvalidFilePathFormat {
            msg: "can not find parent dir of file".to_string(),
        })
}

// normalized_path normalized the given path
// canonicalized to an absolute real path
// It normalizes a file path into a canonical absolute path, while ensuring the parent directory exists.
// So "./data/manifest" becomes something like "/abs/path/data/manifest", and the parent dir is created if needed.
pub(crate) fn normalized_path(p: &str) -> Result<String, KVLogError> {
    let path = Path::new(p);
    // if p is just a filename, treat it like a current directory
    let parent = path.parent().unwrap_or(Path::new("."));
    std::fs::create_dir_all(parent)?;
    let canon_parent = parent.canonicalize()?;
    let file = path.file_name().ok_or(KVLogError::InvalidFilePathFormat {
        msg: "bad path".into(),
    })?;
    Ok(canon_parent.join(file).to_string_lossy().to_string())
}
