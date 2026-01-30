use crate::KVLogError;
use crate::utils::{ANTI_MULTI_PROCESS_LOCK_FILE_NAME, get_parent_dir};
use fs2::FileExt;
use std::fs::{OpenOptions as std_OpenOptions, create_dir_all};

// acquire_file_lock acquires a file lock on the given path for file kvlog.lock
pub(crate) fn acquire_file_lock(p: &str) -> Result<std::fs::File, KVLogError> {
    let parent = &get_parent_dir(p)?;
    create_dir_all(parent)?;
    let path = parent.join(ANTI_MULTI_PROCESS_LOCK_FILE_NAME);

    // it is used for file lock between multiple processes
    // a std::fs::OpenOptions is enough, no need for tokio::fs::OpenOptions
    // let path = path.with_extension("lock");
    let file = std_OpenOptions::new()
        // this will only create a file if the parent directory exists
        // non‑existent directory will fail to create the file
        .create(true)
        .read(true)
        .write(true)
        .open(path)?;
    // if another process is trying to acquire this lock while it is already held by one process, it will return an IO error
    file.try_lock_exclusive()?;

    Ok(file)
}
