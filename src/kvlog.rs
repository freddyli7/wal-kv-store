use crate::KVLogError;
use crate::KVStore;
use crate::lock::acquire_file_lock;
use crate::wal::{
    CHECKSUM_LEN, ENTRY_PREFIX_LEN, MAX_ENTRY_SIZE, WALEntry, crc32, parse_prefix_bytes,
};
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::collections::HashMap;
use std::hash::Hash;
use std::io::SeekFrom;
use std::sync::Arc;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::RwLock;

pub struct KVLog<K, V> {
    // RwLock: one write lock or multiple read locks at a time
    // Writer lock held → no reads, no other writes;
    // Read lock held → any number of reads, no write and writers must wait until all readers release their lock;
    // No lock → any number of reads or a single writer
    mem: Arc<RwLock<HashMap<K, V>>>,
    log: Arc<RwLock<File>>, // Arc makes it shareable across threads / async tasks.
    // _file_lock will keep the file lock held until KVLog is dropped.
    _file_lock: std::fs::File,
}

impl<K, V> KVLog<K, V>
where
    K: Hash + Eq + bincode::Encode + bincode::Decode<()>,
    V: bincode::Encode + bincode::Decode<()>,
{
    /// load will initialize a KVLog from the given path or creating a new one if it does not exist.
    /// During loading, no reading or writing is allowed.
    ///
    /// The data structure here in WAL is:
    /// [4 bytes prefix-length][4 bytes checksum][encoded entry][4 bytes prefix-length][4 bytes checksum][encoded entry]...[4 bytes prefix-length][4 bytes checksum][encoded entry]
    /// 4 bytes is the length prefix of each entry
    /// entries are encoded using bincode
    ///
    /// example:
    /// SET(1,2) -> encode -> [5f,12,42,43,56,22,44] -> len([5f,12,42,43,56,22,44]) -> [01,02,03,04] -> [01,02,03,04] is the len of the entry ->
    /// CRC32 checksum: [5f,12,42,43,56,22,44] -> [aa,bb,cc,dd]
    /// write into WAL will be [len][checksum][payload]: [01,02,03,04,aa,bb,cc,dd,5f,12,42,43,56,22,44]
    /// DELETE(1) -> encode -> [1c,3a,4b,11,25,54,65] -> len([1c,3a,4b,11,25,54,65]) -> [00,00,00,1a] -> [00,00,00,1a] is the len of the entry ->
    /// CRC32 checksum: [1c,3a,4b,11,25,54,65] -> [ff,aa,33,aa]
    /// write into WAL will be [len][checksum][payload]: [00,00,00,1a,ff,aa,33,aa,1c,3a,4b,11,25,54,65]
    /// so WAL will be stored like: [01,02,03,04,aa,bb,cc,dd,5f,12,42,43,56,22,44,00,00,00,1a,ff,aa,33,aa,1c,3a,4b,11,25,54,65] which represent two entries:
    /// SET(1,2)
    /// DELETE(1)
    pub async fn load(path: &str) -> Result<Self, KVLogError> {
        // acquires the file lock, so no other task(process) can access the file until this lock is released.
        // store the lock handle inside KVLog so that lock stays held for the lifetime of the struct.
        let _file_lock = acquire_file_lock(path)?;

        // open WAL or create one if missing
        let file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(path)
            .await?;

        let log = Arc::new(RwLock::new(file));
        let mem = Arc::new(RwLock::new(HashMap::new()));

        // recover
        {
            // log lock: currency read but exclusive write lock
            // now write lock is acquired, so no other reads nor writes are allowed until this lock is released.
            let mut f = log.write().await;
            f.seek(SeekFrom::Start(0)).await?;

            // load everything in WAL into buffer
            let mut buf = Vec::new();
            f.read_to_end(&mut buf).await?;

            // the cumulative size of the valid entry in WAL, used to truncate the WAL file to the size that excludes the partial entry at the end.
            let mut offset = 0usize;
            let mut cursor = &buf[..]; // can also be &buf
            while cursor.len() >= ENTRY_PREFIX_LEN {
                // read length prefix, consistently make it 4 bytes which is u32 regardless the arch(could be 8 bytes if u64)
                let (payload_len_bytes, rest) = cursor.split_at(ENTRY_PREFIX_LEN);

                // this will eliminate the unwrap() so that make this production ready:
                // original: let len_prefix = u32::from_le_bytes(len_prefix_bytes.try_into().unwrap()) as usize;
                let payload_len = parse_prefix_bytes(payload_len_bytes)? as usize;
                if payload_len > MAX_ENTRY_SIZE {
                    return Err(KVLogError::InvalidPrefix {
                        msg: format!(
                            "load entry failed: entry prefix too large: {} > {}",
                            payload_len, MAX_ENTRY_SIZE
                        ),
                    });
                }

                // check of partial entry (crash safe). Partial entries should only appear at the end bcs this is a single writer append only WAL.
                // 4 represents the length of the CRC32 checksum,
                // so, the minimum length of a valid entry should be >= CHECKSUM_LEN + payload_len
                if rest.len() < CHECKSUM_LEN + payload_len {
                    // ignore the partial entry as it is the end of the WAL
                    break;
                }

                let (checksum_bytes, rest) = rest.split_at(CHECKSUM_LEN);
                let loaded_checksum = parse_prefix_bytes(checksum_bytes)?;

                // read entry
                let (payload_bytes, next) = rest.split_at(payload_len);
                let recomputed_checksum = crc32(payload_bytes);

                // CRC32 checksum verification for each entry loaded
                // this is to make sure the entry is not corrupted
                // in the case that the last entry is corrupted and still with correct prefix length and payload bytes size, it will pass the size check: rest.len() < CHECKSUM_LEN + len_prefix
                // but, it is a partial entry so we should not return error here, we should treat it the same way as the torn tail.
                if loaded_checksum != recomputed_checksum {
                    // there are still bytes remaining, so checksum mismatch happens in the middle, we are sure that the WAL is corrupted
                    if next.len() > 0 {
                        return Err(KVLogError::CorruptWal(
                            "load entry failed: entry checksum mismatch".to_string(),
                        ));
                    } else {
                        // no more bytes remain, so the checksum mismatch must mean the torn entry at the end
                        // handle it by ignoring the partial entry at the end
                        break;
                    }
                }

                // deserialize entry
                let (entry, _): (WALEntry<K, V>, usize) =
                    bincode::decode_from_slice(payload_bytes, bincode::config::standard())?;

                // apply to mem
                // release and reacquire mem.write() each entry, so if some other task had access to mem at the same time, it could read a partially‑recovered state.
                // however, KVLog has not been returned during the recovering, so even if read lock of mem is acquired, there is nothing to read.
                {
                    let mut mem_guard = mem.write().await;
                    match entry {
                        WALEntry::Set { key, value } => {
                            mem_guard.insert(key, value);
                        }
                        WALEntry::Delete { key } => {
                            mem_guard.remove(&key);
                        }
                    }
                }

                // add up each valid entry size
                // this will ignore the partial entry at the end
                offset += ENTRY_PREFIX_LEN + CHECKSUM_LEN + payload_len;

                cursor = next;
            }
            // Truncates or extends the underlying file, updating the size of this file to become size.
            f.set_len(offset as u64).await?;
        }

        // only the recovering id completed and KVLog is returned from here, other thread can acquire the mem lock
        Ok(KVLog {
            mem,
            log,
            _file_lock,
        })
    }
}

impl<K, V> KVStore<K, V> for KVLog<K, V>
where
    K: Serialize
        + DeserializeOwned
        + Send
        + Sync
        + bincode::Encode
        + bincode::Decode<()>
        + Clone
        + Eq
        + Hash,
    V: Serialize + DeserializeOwned + Send + Sync + bincode::Encode + bincode::Decode<()> + Clone,
{
    /// get returns the value associated with the given key -> Ok(Some(value)), or Ok(None) if the key does not exist.
    /// KVLogError for any error
    async fn get(&self, key: K) -> Result<Option<V>, KVLogError> {
        let read_guard = self.mem.read().await;
        Ok(read_guard.get(&key).cloned())
    }

    /// set_with_flush does write and fsync per each entry
    ///
    /// this is slow but provides a strong guarantee that each entry is durable.
    /// flush returns only after WAL is fsync’d, so data survives crashes after set_with_flush returns.
    /// Reads may see the old value until set_with_flush completes
    async fn set_with_flush(&self, key: K, value: V) -> Result<Option<V>, KVLogError> {
        // create a log entry
        let entry = WALEntry::Set {
            key: key.clone(),
            value: value.clone(),
        };

        // encode the log entry into raw bytes
        let encoded = bincode::encode_to_vec(entry, bincode::config::standard())?;
        // make sure the encoded entry len is u32 so that load() can use 4 bytes as the framing
        let prefix_len: u32 = encoded.len().try_into()?;
        // generate CRC32 checksum
        let checksum = crc32(&*encoded); // todo: trait
        let checksum_bytes = checksum.to_le_bytes();

        let mut to_write = prefix_len.to_le_bytes().to_vec();
        // extend() takes any iterator/collection and appends its items. It moves the encoded.
        // append() specifically takes &mut Vec<T>, moves all items, and empties the source vec so that encoded still exists but just empty.
        // data structure: [prefix_len][checksum][encoded]
        to_write.extend(checksum_bytes);
        to_write.extend(encoded);

        // acquire write lock of log
        // writing and fsync to disk

        // keeps the WAL lock held while updating mem, which preserves WAL order in memory
        // for example, this will be prevented:
        //  1. Writer A writes to WAL, releases log lock.
        //  2. Writer B writes to WAL, releases log lock.
        //  3. A then updates mem, then B updates mem — order depends on who gets mem lock first, not WAL order.
        {
            let mut f = self.log.write().await;
            f.write_all(&to_write).await?;
            // A crash or power loss during write_all or before sync_all can leave a partial entry
            // e.g., length prefix is written but not the full payload
            // partial entries should only appear at the end

            // If the crash happens before write_all starts, then no new bytes are written — no partial entry.
            // If the crash happens during write_all, you can get a partial entry.
            // If it happens after write_all but before sync_all, the entry might be fully in the OS page cache but not durable;
            // after restart it may be missing or partially written, depending on what
            // actually reached disk.

            // write_all writes to the OS buffer; it guarantees the bytes are handed to the kernel, not that they’re on disk.
            // sync_all (fsync) asks the OS to flush those buffers to stable storage.
            // write_all puts bytes into the OS page cache; they’re visible if you read the file,
            // but not guaranteed durable. Only sync_all/fsync makes a persistence guarantee.
            f.sync_all().await?;

            // acquire write lock of mem
            // as soon as the writer acquires mem.write() to update the HashMap,
            // any readers trying to acquire mem.read() will block until the writer releases that lock.
            let mut mem = self.mem.write().await;
            // HashMap insert API: if key not exist, return None, otherwise return old value
            Ok(mem.insert(key, value))
        }
    }

    /// set_without_flush only appends to WAL(in the OS buffer) and updates the mem, but does not fsync
    ///
    /// this is faster than set_with_flush but not durable, so any unflushed data in os buffer can be lost on crashing or power loss.
    /// OS buffer will automatically flush but could be at any time, it is not deterministic
    /// call flush() to batch flush all prior writes in the os buffer to disk.
    async fn set_without_flush(&self, key: K, value: V) -> Result<Option<V>, KVLogError> {
        let entry = WALEntry::Set {
            key: key.clone(),
            value: value.clone(),
        };

        let encoded = bincode::encode_to_vec(entry, bincode::config::standard())?;
        let prefix_len: u32 = encoded.len().try_into()?;
        let checksum = crc32(&*encoded);
        let checksum_bytes = checksum.to_le_bytes();
        let mut to_write = prefix_len.to_le_bytes().to_vec();
        to_write.extend(checksum_bytes);
        to_write.extend(encoded);

        {
            let mut f = self.log.write().await;
            f.write_all(&to_write).await?;

            let mut mem = self.mem.write().await;
            Ok(mem.insert(key, value))
        }
    }

    /// delete_with_flush appends the operation to log and removes the key from the mem
    /// if key not present, return Ok(None) without entry log, but this is not an error.
    /// Trade-offs: readers are blocked during the fsync because you hold mem.write() while flushing.
    /// Readers in delete are blocked longer than they are in set_with_flush if the key presents.
    /// No WAL entry for missing keys.
    /// Durability before visibility.
    async fn delete_with_flush(&self, key: K) -> Result<Option<V>, KVLogError> {
        {
            let mem_lock = self.mem.read().await;
            if mem_lock.get(&key).is_none() {
                return Ok(None);
            }
        }

        let entry: WALEntry<K, V> = WALEntry::Delete { key: key.clone() };

        // encode the log entry into raw bytes
        let encoded = bincode::encode_to_vec(entry, bincode::config::standard())?;
        // make sure the encoded entry len is u32 so that load() can use 4 bytes as the framing
        let len_prefix: u32 = encoded.len().try_into()?;
        // generate CRC32 checksum
        let checksum = crc32(&*encoded);
        let checksum_bytes = checksum.to_le_bytes();

        let mut to_write = len_prefix.to_le_bytes().to_vec();
        to_write.extend(checksum_bytes);
        to_write.extend(encoded);

        {
            // always keep the lock order the same to avoid deadlock
            let mut f = self.log.write().await;
            // check if the key exists
            let mut mem_lock = self.mem.write().await;

            if mem_lock.get(&key).is_none() {
                // key not exist, return without entry log, but this is not an error
                return Ok(None);
            }

            f.write_all(&to_write).await?;
            f.sync_all().await?;

            let re = mem_lock.remove(&key);
            Ok(re)
        }
    }

    /// delete_without_flush only appends to WAL(in the OS buffer) and updates the mem, but does not fsync
    ///
    /// this is faster than delete_with_flush but not durable, so any unflushed data in os buffer can be lost on crashing or power loss.
    /// OS buffer will automatically flush but could be at any time, it is not deterministic
    /// call flush() to batch flush all prior writes in the os buffer to disk.
    async fn delete_without_flush(&self, key: K) -> Result<Option<V>, KVLogError> {
        {
            let mem_lock = self.mem.read().await;
            if mem_lock.get(&key).is_none() {
                return Ok(None);
            }
        }

        let entry: WALEntry<_, V> = WALEntry::Delete { key: key.clone() };

        let encoded = bincode::encode_to_vec(entry, bincode::config::standard())?;
        let len_prefix: u32 = encoded.len().try_into()?;

        // generate CRC32 checksum
        let checksum = crc32(&*encoded);
        let checksum_bytes = checksum.to_le_bytes();

        let mut to_write = len_prefix.to_le_bytes().to_vec();
        to_write.extend(checksum_bytes);
        to_write.extend(encoded);

        {
            let mut f = self.log.write().await;

            let mut mem_lock = self.mem.write().await;
            if mem_lock.get(&key).is_none() {
                return Ok(None);
            }

            f.write_all(&to_write).await?;

            let re = mem_lock.remove(&key);
            Ok(re)
        }
    }

    // flush does manual fsync to make all prior writes in the os buffer durable.
    async fn flush(&self) -> Result<(), KVLogError> {
        let f = self.log.write().await;
        f.sync_all().await?;

        Ok(())
    }
}

// please include the following:
// - brief description of implementation decisions, including:
//   - what is persisted (files, directories) and any significant tradeoffs
//   - choices about contention and access control (e.g. Mutexes, Marker files, etc.)
//   - assurances that recovery will always be in a good state, e.g. no partial writes
// - basic tests for the above properties
// - bonus: tests with multiple async tasks, single and multi-threaded executor
// - extra bonus: thoughts on the interface (e.g. trait_variant, non-mut get and delete, return value on set, etc.)
#[cfg(test)]
mod tests {
    use crate::{KVLog, KVStore, wal::crc32};
    use std::collections::HashSet;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::SystemTime;
    use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, SeekFrom};

    struct Cleanup {
        path: PathBuf,
    }

    impl Drop for Cleanup {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.path);
        }
    }

    fn unique_suffix() -> PathBuf {
        let mut path = std::env::temp_dir();
        let nanos = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        path.push(nanos.to_string());
        path
    }

    async fn setup(path: &str) -> KVLog<String, String> {
        KVLog::<String, String>::load(path)
            .await
            .expect("load failed")
    }

    async fn read_wal_entries(log: &KVLog<String, String>) -> Vec<super::WALEntry<String, String>> {
        let mut f = log.log.write().await;
        f.seek(SeekFrom::Start(0)).await.expect("seek failed");

        let mut buf = Vec::new();
        f.read_to_end(&mut buf).await.expect("read failed");

        let mut entries = Vec::new();
        let mut cursor = &buf[..];
        while cursor.len() >= 4 {
            let (len_prefix_bytes, rest) = cursor.split_at(4);
            let len_prefix = u32::from_le_bytes(len_prefix_bytes.try_into().unwrap()) as usize;

            if rest.len() < 4 + len_prefix {
                break;
            }

            let (checksum_bytes, rest) = rest.split_at(4);
            let loaded_checksum = u32::from_le_bytes(checksum_bytes.try_into().unwrap());

            let (entry_bytes, next) = rest.split_at(len_prefix);
            let recomputed_checksum = crc32(entry_bytes);

            if loaded_checksum != recomputed_checksum {
                break;
            }

            let entry: super::WALEntry<String, String> =
                match bincode::decode_from_slice(entry_bytes, bincode::config::standard()) {
                    Ok((e, _)) => e,
                    Err(_) => break,
                };
            entries.push(entry);
            cursor = next;
        }

        entries
    }

    async fn write_wal_entries(path: &str, entries: &[super::WALEntry<String, String>]) {
        let mut file = tokio::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path)
            .await
            .expect("open wal failed");

        for entry in entries {
            let encoded =
                bincode::encode_to_vec(entry, bincode::config::standard()).expect("encode failed");
            let len_prefix: u32 = encoded.len().try_into().expect("encode failed: too large");
            let checksum = crc32(&*encoded);
            let checksum_bytes = checksum.to_le_bytes();
            let mut to_write = len_prefix.to_le_bytes().to_vec();
            to_write.extend(checksum_bytes);
            to_write.extend(encoded);
            file.write_all(&to_write).await.expect("wal write failed");
        }

        file.sync_all().await.expect("wal sync failed");
    }

    fn parse_key(key: &str) -> (usize, usize) {
        let mut parts = key.split('_');
        let writer_part = parts.next().expect("missing writer key");
        let round_part = parts.next().expect("missing round key");
        let writer_idx: usize = writer_part
            .strip_prefix('k')
            .expect("bad writer key")
            .parse()
            .expect("bad writer idx");
        let round_idx: usize = round_part.parse().expect("bad round idx");
        (writer_idx, round_idx)
    }

    #[tokio::test]
    async fn test_set_get_delete() {
        // setup and cleanup
        let log_path = unique_suffix();
        // the declaration order matters here:
        // define _cleanup then log so that log will be dropped first
        // bcs _cleanup will try to remove the log file and if the log file is still open by log var,
        // it will fail on windows as you can't delete a file while it's still open
        let _cleanup = Cleanup {
            path: log_path.clone(),
        }; // _cleanup will drop even if the test failed
        let log = setup(log_path.to_str().expect("log path is not valid UTF-8")).await;

        let key = "foo".to_string();
        let v1 = "bar".to_string();
        let v2 = "aha".to_string();

        let mut get_result = log.get(key.clone()).await;
        assert_eq!(
            get_result.unwrap(),
            None,
            "get should return None before set"
        );

        // new key set will return None which is the old value
        let mut set_result = log.set_with_flush(key.clone(), v1.clone()).await;
        assert!(
            set_result.unwrap().is_none(),
            "set should return old value aka None"
        );

        get_result = log.get(key.clone()).await;
        assert_eq!(
            get_result.unwrap(),
            Some(v1.clone()),
            "get should return value bar"
        );

        set_result = log.set_with_flush(key.clone(), v2.clone()).await;
        assert_eq!(
            set_result.unwrap(),
            Some(v1),
            "set should return old value aka bar"
        );

        get_result = log.get(key.clone()).await;
        assert_eq!(
            get_result.unwrap(),
            Some(v2),
            "get should return new value aha"
        );

        log.delete_with_flush(key.clone()).await.unwrap();
        assert!(
            log.get(key).await.unwrap().is_none(),
            "get should return None after delete"
        );
    }

    #[tokio::test]
    async fn test_load_replays_wal() {
        let log_path = unique_suffix();
        let _cleanup = Cleanup {
            path: log_path.clone(),
        };

        let entries = vec![
            super::WALEntry::Set {
                key: "a".to_string(),
                value: "1".to_string(),
            },
            super::WALEntry::Set {
                key: "b".to_string(),
                value: "2".to_string(),
            },
            super::WALEntry::Set {
                key: "a".to_string(),
                value: "3".to_string(),
            },
            super::WALEntry::Delete {
                key: "b".to_string(),
            },
        ];

        write_wal_entries(
            log_path.to_str().expect("log path is not valid UTF-8"),
            &entries,
        )
        .await;

        let log = setup(log_path.to_str().expect("log path is not valid UTF-8")).await;

        assert_eq!(
            log.get("a".to_string()).await.unwrap(),
            Some("3".to_string())
        );
        assert_eq!(log.get("b".to_string()).await.unwrap(), None);

        let read_back = read_wal_entries(&log).await;
        assert_eq!(read_back.len(), entries.len());
        for (expected, actual) in entries.into_iter().zip(read_back.into_iter()) {
            match (expected, actual) {
                (
                    super::WALEntry::Set { key: ek, value: ev },
                    super::WALEntry::Set { key: ak, value: av },
                ) => {
                    assert_eq!(ek, ak);
                    assert_eq!(ev, av);
                }
                (super::WALEntry::Delete { key: ek }, super::WALEntry::Delete { key: ak }) => {
                    assert_eq!(ek, ak);
                }
                _ => panic!("mismatched WAL entry types"),
            }
        }
    }

    // many writers + many readers running at the same time, each in its own Tokio worker thread,
    // all sharing one log file, and verifying that writes are serialized while reads proceed concurrently from memory.
    #[tokio::test(flavor = "multi_thread", worker_threads = 20)]
    async fn multi_thread_test() {
        // setup and cleanup
        let log_path = unique_suffix();
        // the declaration order matters here:
        // define _cleanup then log so that log will be dropped first
        // bcs _cleanup will try to remove the log file and if the log file is still open by log var,
        // it will fail on windows as you can't delete a file while it's still open
        let _cleanup = Cleanup {
            path: log_path.clone(),
        }; // _cleanup will drop even if the test failed
        let log = setup(log_path.to_str().expect("log path is not valid UTF-8")).await;

        // wrap log in Arc so many async tasks can share the same store instance safely
        let log = Arc::new(log);

        let writers = 10;
        let readers = 10;
        let deletes = 5;
        // 20 writes per writer
        let rounds = 20;

        // Creates a barrier that waits for all tasks (writers + readers) before starting.
        // wrap it with Arc so every task can share the same barrier.
        let start = Arc::new(tokio::sync::Barrier::new(writers + readers));
        // JoinSet manages multiple spawned tasks and lets you await them.
        let mut set = tokio::task::JoinSet::new();

        for w in 0..writers {
            // Each task clones log and start for its own use.
            let log = log.clone();
            let start = start.clone();
            // spawn a task for each writer
            set.spawn(async move {
                // start.wait().await blocks until all tasks reach the barrier, so writers/readers begin together.
                start.wait().await;
                for r in 0..rounds {
                    let key = format!("k{}_{}", w, r);
                    let value = format!("v{}_{}", w, r);
                    log.set_with_flush(key, value).await;
                    if w == 0 && r < deletes {
                        let delete_key = format!("k{}_{}", w, r);
                        log.delete_with_flush(delete_key).await;
                    }
                }
            });
        }

        for r in 0..readers {
            let log = log.clone();
            let start = start.clone();
            set.spawn(async move {
                start.wait().await;
                // spawn a task for each reader
                for w in 0..writers {
                    let key = format!("k{}_{}", w, r % rounds);
                    let _ = log.get(key).await;
                }
            });
        }

        // run tasks in JoinSet and waits for all tasks to finish.
        while let Some(res) = set.join_next().await {
            res.unwrap();
        }

        // Validate that all keys are present in memory after all writers finish.
        for w in 0..writers {
            for r in 0..rounds {
                let key = format!("k{}_{}", w, r);
                let expected = format!("v{}_{}", w, r);
                if w == 0 && r < deletes {
                    assert_eq!(log.get(key).await.unwrap(), None);
                } else {
                    assert_eq!(log.get(key).await.unwrap(), Some(expected));
                }
            }
        }

        // Validate WAL contents: all entries exist and each writer's sequence is ordered.
        let entries = read_wal_entries(&log).await;
        assert_eq!(entries.len(), (writers * rounds) + deletes);

        let mut seen = HashSet::new();
        let mut last_round = vec![None::<usize>; writers];
        let mut set_count = 0;
        let mut delete_count = 0;
        let mut seen_set_keys = HashSet::new();

        for entry in entries {
            match entry {
                super::WALEntry::Set { key, value } => {
                    let (writer_idx, round_idx) = parse_key(&key);
                    assert_eq!(value, format!("v{}_{}", writer_idx, round_idx));
                    assert!(seen.insert(key));
                    seen_set_keys.insert((writer_idx, round_idx));
                    set_count += 1;

                    if let Some(prev) = last_round[writer_idx] {
                        assert_eq!(
                            round_idx,
                            prev + 1,
                            "writer {} out of order: {} then {}",
                            writer_idx,
                            prev,
                            round_idx
                        );
                    }
                    last_round[writer_idx] = Some(round_idx);
                }
                super::WALEntry::Delete { key } => {
                    let (writer_idx, round_idx) = parse_key(&key);
                    assert!(writer_idx == 0 && round_idx < deletes);
                    assert!(
                        seen_set_keys.contains(&(writer_idx, round_idx)),
                        "delete before set for {}",
                        key
                    );
                    delete_count += 1;
                }
            }
        }

        assert_eq!(set_count, writers * rounds);
        assert_eq!(delete_count, deletes);
    }

    // TODO:
    // production ready:
    // - Crash safety
    //       - Handle partial/torn WAL entries without panic - return error and let the caller decide what to do. Done
    //       - Checksums or CRC per entry to detect corruption. [prefix-length][checksum][payload] Done
    //       - Recovery tests for mid‑write crashes.
    //   - Error handling
    //       - No expect in core paths; return typed errors. Done
    //       - Propagate fsync / IO errors to callers. Done
    //   - Durability semantics
    //       - Clear guarantees (fsync policy, when writes are visible). Document
    //       - Optional batched/async flush mode with explicit flush(). Done
    //   - Concurrency & correctness
    //       - Strict ordering guarantees. Write is strictly ordered, read is not. Done
    //       - Tests for concurrent writers/readers/deletes across threads.
    //       - Defined behavior for read‑your‑write and visibility. Done
    //   - Resource management
    //       - WAL compaction / snapshotting to cap log growth.
    //       - Backpressure or size limits to avoid disk exhaustion.
    //   - Operational safety
    //       - File locking to prevent multi‑process writers. Impl done, need multi-processes tests
    //       - Define KVLogError type Done
    //   - Testing
    //       - Fuzz WAL decode.
    //       - Property tests for ordering & idempotence.
    //       - Load/recovery tests with random failures.
    //   - Documentation
    //       - Explicit guarantees (durability, consistency, concurrency).
    //       - Known limitations (e.g., single‑process only).
}
