use crate::kvlog::KVLog;
use crate::traits::Snapshot;
use crate::{KVLogError, KVStore};
use bincode::Encode;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::hash::Hash;
use std::sync::Arc;

// RAII guard
// The guard is dropped automatically at the end of the loop iteration (or if the task is cancelled/panics)
struct SnapshotFlagGuard<'a, K, V>
where
    K: Clone + Encode,
    V: Clone + Encode,
{
    // it takes a reference of the obj
    flag: &'a KVLog<K, V>,
}

// guard impl the Drop trait, which is called when the guard goes out of scope
impl<'a, K, V> Drop for SnapshotFlagGuard<'a, K, V>
where
    K: Clone + Encode,
    V: Clone + Encode,
{
    fn drop(&mut self) {
        self.flag.store_snapshot_flag(false);
    }
}

pub struct KVLogHandler<K: Clone + Encode, V: Clone + Encode>(Arc<KVLog<K, V>>);

impl<K, V> KVLogHandler<K, V>
where
    K: Serialize
        + DeserializeOwned
        + Send
        + Sync
        + bincode::Encode
        + bincode::Decode<()>
        + Clone
        + Eq
        + Hash
        + 'static,
    V: Serialize
        + DeserializeOwned
        + Send
        + Sync
        + bincode::Encode
        + bincode::Decode<()>
        + Clone
        + 'static,
{
    pub async fn load(manifest_path: &str) -> Result<Self, KVLogError> {
        let (log, mut rx) = KVLog::load(manifest_path).await?;
        let log = Arc::new(log);
        let arc_log = Arc::clone(&log);
        tokio::spawn(async move {
            while rx.recv().await.is_some() {
                if arc_log.swap_snapshot_flag(true) {
                    continue;
                }

                let _guard = SnapshotFlagGuard {
                    flag: arc_log.as_ref(),
                };

                if let Err(e) = arc_log.on_snapshot().await {
                    eprintln!("snapshot failed: {e}");
                }

                // _Drop clears _guard even if task is canceled/panic
            }
        });

        Ok(KVLogHandler(log))
    }
}

impl<K, V> KVStore<K, V> for KVLogHandler<K, V>
where
    K: Serialize
        + DeserializeOwned
        + Send
        + Sync
        + bincode::Encode
        + bincode::Decode<()>
        + Clone
        + Eq
        + Hash
        + 'static,
    V: Serialize
        + DeserializeOwned
        + Send
        + Sync
        + bincode::Encode
        + bincode::Decode<()>
        + Clone
        + 'static,
{
    async fn get(&self, key: K) -> Result<Option<V>, KVLogError> {
        <KVLog<K, V> as KVStore<K, V>>::get(self.0.as_ref(), key).await
    }

    async fn set_with_flush(&self, key: K, value: V) -> Result<Option<V>, KVLogError> {
        <KVLog<K, V> as KVStore<K, V>>::set_with_flush(self.0.as_ref(), key, value).await
    }

    async fn set_without_flush(&self, key: K, value: V) -> Result<Option<V>, KVLogError> {
        <KVLog<K, V> as KVStore<K, V>>::set_without_flush(self.0.as_ref(), key, value).await
    }

    async fn delete_with_flush(&self, key: K) -> Result<Option<V>, KVLogError> {
        <KVLog<K, V> as KVStore<K, V>>::delete_with_flush(self.0.as_ref(), key).await
    }

    async fn delete_without_flush(&self, key: K) -> Result<Option<V>, KVLogError> {
        <KVLog<K, V> as KVStore<K, V>>::delete_without_flush(self.0.as_ref(), key).await
    }

    async fn flush(&self) -> Result<(), KVLogError> {
        <KVLog<K, V> as KVStore<K, V>>::flush(self.0.as_ref()).await
    }
}

impl<K, V> Snapshot for KVLogHandler<K, V>
where
    K: Serialize
        + DeserializeOwned
        + Send
        + Sync
        + bincode::Encode
        + bincode::Decode<()>
        + Clone
        + Eq
        + Hash
        + 'static,
    V: Serialize
        + DeserializeOwned
        + Send
        + Sync
        + bincode::Encode
        + bincode::Decode<()>
        + Clone
        + 'static,
{
    async fn on_snapshot(&self) -> Result<bool, KVLogError> {
        self.0.as_ref().on_snapshot().await
    }
}
