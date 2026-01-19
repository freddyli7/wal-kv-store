use crate::KVLogError;
use fs2::FileExt;
use std::fs::OpenOptions as std_OpenOptions;

pub(crate) fn acquire_file_lock(p: &str) -> Result<std::fs::File, KVLogError> {
    let path = std::path::Path::new(p);

    let lock_path = path.with_extension("lock");
    let file = std_OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(lock_path)?;
    file.try_lock_exclusive()?;

    Ok(file)
}
