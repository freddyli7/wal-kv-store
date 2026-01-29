//! implement a version of struct KVLog that implements KVStore with the properties:
//! - reentrant (when shared across either threads or async tasks) - concurrent access to the same KVLog instance is safe
//! - backed by a log (filesystem is enough for this purpose)
//! - persistence is guaranteed before access (e.g. Write Ahead Log or equivalent guarantee) = durability before visibility - a single‑writer append‑only WAL(concurrent writers at the API level, but a single writer at the WAL I/O level)
//! - will load the persisted state on startup

mod error;
mod kvlog;
mod lock;
mod manifest;
mod traits;
mod utils;

pub use error::KVLogError;
pub use kvlog::KVLog;
pub use traits::KVStore;
