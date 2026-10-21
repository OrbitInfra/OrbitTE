use std::fmt;
use std::io;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    InvalidConfig(&'static str),
    InvalidRange,
    PermissionDenied,
    RegionNotFound,
    ForeignRegistration,
    RegistrationConflict,
    SourceClosed,
    EpochMismatch,
    ResourceExhausted,
    Protocol(&'static str),
    Timeout,
    Internal(&'static str),
    Io(io::Error),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfig(message) => write!(f, "invalid configuration: {message}"),
            Self::InvalidRange => {
                f.write_str("transfer range is empty, overflowing or outside its registration")
            }
            Self::PermissionDenied => {
                f.write_str("registration does not permit the remote operation")
            }
            Self::RegionNotFound => f.write_str("registration is no longer exported"),
            Self::ForeignRegistration => {
                f.write_str("registration belongs to another engine or segment")
            }
            Self::RegistrationConflict => {
                f.write_str("overlapping registrations must share the same HostMemory owner")
            }
            Self::SourceClosed => f.write_str("source admission is closed"),
            Self::EpochMismatch => {
                f.write_str("source epoch does not match authoritative ownership")
            }
            Self::ResourceExhausted => f.write_str("engine resource budget exhausted"),
            Self::Protocol(message) => write!(f, "invalid OrbitTE frame: {message}"),
            Self::Timeout => f.write_str("operation deadline expired"),
            Self::Internal(message) => write!(f, "internal engine failure: {message}"),
            Self::Io(error) => write!(f, "transport I/O failed: {error}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}

impl From<io::Error> for Error {
    fn from(error: io::Error) -> Self {
        match error.kind() {
            io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock => Self::Timeout,
            _ => Self::Io(error),
        }
    }
}
