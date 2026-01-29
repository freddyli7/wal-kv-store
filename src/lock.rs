use crate::KVLogError;
use fs2::FileExt;
use std::fs::OpenOptions as std_OpenOptions;

// normalized_path normalized the given path
// canonicalize to an absolute real path
pub(crate) fn normalized_path(p: &str) -> Result<String, KVLogError> {
    let path = std::path::Path::new(p);
    // if p is just a filename, treat it like a current directory
    let parent = path.parent().unwrap_or(std::path::Path::new("."));
    std::fs::create_dir_all(parent)?;
    let canon_parent = parent.canonicalize()?;
    let file = path.file_name().ok_or(KVLogError::InvalidFilePathFormat { msg: "bad path".into() })?;
    Ok(canon_parent.join(file).to_string_lossy().to_string())
}

// acquire_file_lock acquires a file lock on the given path
pub(crate) fn acquire_file_lock(p: &str) -> Result<std::fs::File, KVLogError> {
    let path = std::path::Path::new(p);

    // this will create a file with .lock extension
    // it is used for file lock between multiple processes
    // a std::fs::OpenOptions is enough, no need for tokio::fs::OpenOptions
    let lock_path = path.with_extension("lock");
    let file = std_OpenOptions::new()
        // this will only create a file if the parent directory exists
        // non‑existent directory will fail to create the file
        .create(true)
        .read(true)
        .write(true)
        .open(lock_path)?;
    // if another process is trying to acquire this lock while it is already held by one process, it will return an IO error
    file.try_lock_exclusive()?;

    Ok(file)
}
