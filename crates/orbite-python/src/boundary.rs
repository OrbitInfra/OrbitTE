use orbite::{Error, NotificationEffect, NotificationFailure};
use std::ffi::{c_char, c_int};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::ptr::NonNull;
use std::sync::{Mutex, MutexGuard};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Code(pub c_int);
pub(crate) type Result<T> = std::result::Result<T, Code>;
pub(crate) const INVALID: Code = Code(1);
pub(crate) const FULL: Code = Code(2);
pub(crate) const ADDRESS: Code = Code(3);
pub(crate) const UNSENT: Code = Code(134);
pub(crate) const UNKNOWN: Code = Code(135);
pub(crate) const INTERNAL: Code = Code(199);
pub(crate) const UNSUPPORTED: Code = Code(200);

impl From<Error> for Code {
    fn from(e: Error) -> Self {
        Self::from(&e)
    }
}
impl From<&Error> for Code {
    fn from(e: &Error) -> Self {
        match e {
            Error::ResourceExhausted | Error::BatchBusy | Error::BatchOpen => FULL,
            Error::RegionNotFound | Error::ForeignRegistration => ADDRESS,
            Error::EpochMismatch => Code(7),
            Error::Unquiesced { .. } => UNKNOWN,
            Error::Unavailable(_) | Error::TransportPolicy { .. } => UNSUPPORTED,
            Error::Backend { operation, .. }
                if operation.starts_with("cu") || operation.contains("CUDA") =>
            {
                Code(101)
            }
            Error::Backend { operation, .. }
                if operation.contains("RDMA") || operation.starts_with("ibv") =>
            {
                Code(100)
            }
            Error::Protocol(_) | Error::Io(_) | Error::Timeout => Code(132),
            Error::InvalidRange
            | Error::InvalidTask
            | Error::PermissionDenied
            | Error::InvalidConfig(_)
            | Error::InvalidSelectorConfig(_)
            | Error::InvalidQosConfig(_)
            | Error::RegistrationConflict
            | Error::BatchClosed
            | Error::FileSegmentClosed
            | Error::ForeignQosPolicy => INVALID,
            Error::TentConfig { path, .. }
                if matches!(path.as_str(), "metadata_type" | "metadata_servers") =>
            {
                Code(6)
            }
            Error::TentConfig { reason, .. } if reason.contains("JSON") => Code(133),
            Error::TentConfig { reason, .. } if reason.contains("unsupported") => UNSUPPORTED,
            Error::TentConfig { .. } => INVALID,
            Error::SourceClosed
            | Error::RequesterClosed
            | Error::PeerClosed
            | Error::Cancelled
            | Error::DeadlineInfeasible => Code(5),
            _ => INTERNAL,
        }
    }
}
impl From<NotificationFailure> for Code {
    fn from(f: NotificationFailure) -> Self {
        match (f.effect, f.error) {
            (NotificationEffect::Unknown, _) => UNKNOWN,
            (NotificationEffect::None, Error::Io(_) | Error::Timeout) => UNSENT,
            (_, e) => e.into(),
        }
    }
}
pub(crate) fn guard<T>(fallback: T, f: impl FnOnce() -> T) -> T {
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(out) => out,
        Err(payload) => {
            // A foreign logger may panic with a payload whose destructor panics too.
            if let Err(second) = catch_unwind(AssertUnwindSafe(|| drop(payload))) {
                std::mem::forget(second);
            }
            fallback
        }
    }
}
pub(crate) fn lock<T>(m: &Mutex<T>) -> Result<MutexGuard<'_, T>> {
    m.lock().map_err(|_| INTERNAL)
}

// Validate representable bounds before checking the retained exporter extent.
pub(crate) fn checked<T>(p: *mut T, n: usize) -> Result<NonNull<T>> {
    let bytes = n
        .checked_mul(size_of::<T>())
        .filter(|n| *n <= isize::MAX as usize)
        .ok_or(INVALID)?;
    if !p.is_aligned() || (p as usize).checked_add(bytes).is_none() {
        return Err(INVALID);
    }
    NonNull::new(p).ok_or(INVALID)
}
pub(crate) fn fixed<const N: usize>(s: &str) -> [c_char; N] {
    let mut out = [0; N];
    for (dst, src) in out[..N - 1].iter_mut().zip(s.bytes()) {
        *dst = src as c_char;
    }
    out
}
