use thiserror::Error;

#[derive(Debug, Error)]
// Error: implements the std::error::Error trait, so the type behaves like a standard error.
// Debug: lets you format the value with {:?} for debugging output. used for println!("{:?}") or dbg!()
pub enum KVLogError {
    #[error("io error: {0}")] // Display format
    Io(#[from] std::io::Error), // any I/O error becomes KvError::Io.
    #[error("decode error: {0}")]
    Decode(#[from] bincode::error::DecodeError), // decode failures become KvError::Decode.
    #[error("encode error: {0}")]
    Encode(#[from] bincode::error::EncodeError),
    #[error("int conversion error: {0}")]
    Int(#[from] std::num::TryFromIntError),
    #[error("corrupt wal: {0}")]
    CorruptWal(String), // a custom error with your own message.
    #[error("invalid prefix: {msg}")]
    InvalidPrefix { msg: String },
    #[error("key not found: {msg}")]
    KeyNotFound { msg: String },
    #[error("invalid file path format: {msg}")]
    InvalidFilePathFormat { msg: String },
}
