// Given the following trait representing a key-value store

use crate::KVLogError;
use serde::Serialize;
use serde::de::DeserializeOwned;

// async fn traits aren’t object‑safe by default
#[trait_variant::make(KVStore: Send)]
// Send bound: the value can be moved to another thread.
// Sync bound: references to the value can be shared across threads
pub trait LocalKVStore<K, V>
where
    K: Serialize + DeserializeOwned + Send + Sync,
    V: Serialize + DeserializeOwned + Send + Sync,
{
    async fn get(&self, key: K) -> Result<Option<V>, KVLogError>;
    async fn set_with_flush(&self, key: K, value: V) -> Result<Option<V>, KVLogError>;
    async fn set_without_flush(&self, key: K, value: V) -> Result<Option<V>, KVLogError>;
    async fn flush(&self) -> Result<(), KVLogError>;
    async fn delete(&self, key: K) -> Result<Option<V>, KVLogError>;
}
