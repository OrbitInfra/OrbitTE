use orbite::{Error, NotificationEffect, NotificationFailure};
use std::ffi::{c_char, c_int};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::ptr::{self, NonNull};
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
            Error::TentConfig { path, .. } if path == "metadata_type" => Code(6),
            Error::TentConfig { reason, .. } if reason.contains("JSON") => Code(133),
            Error::TentConfig { reason, .. } if reason.contains("unsupported") => UNSUPPORTED,
            Error::TentConfig { .. } => INVALID,
            Error::SourceClosed
            | Error::RequesterClosed
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
pub(crate) fn call(f: impl FnOnce() -> Result<()>) -> c_int {
    guard(-INTERNAL.0, || f().map_or_else(|e| -e.0, |_| 0))
}
pub(crate) fn void_call(name: &str, f: impl FnOnce() -> Result<()>) {
    guard((), || {
        if let Err(code) = f() {
            log::error!("{name} failed ({})", -code.0);
        }
    })
}
pub(crate) fn lock<T>(m: &Mutex<T>) -> Result<MutexGuard<'_, T>> {
    m.lock().map_err(|_| INTERNAL)
}

// Only validate representable bounds here. As with the native C API, the
// caller supplies live readable/writable storage of the specified extent.
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
pub(crate) fn input<'a, T>(p: *const T, n: usize) -> Result<&'a [T]> {
    let p = checked(p.cast_mut(), n)?;
    // SAFETY: the enclosing C call supplies n initialized values, valid for the call.
    Ok(unsafe { std::slice::from_raw_parts(p.as_ptr(), n) })
}
pub(crate) fn put<T>(p: *mut T, v: T) -> Result<()> {
    let p = checked(p, 1)?;
    // SAFETY: the C caller provided writable, aligned output. It may be uninitialized.
    unsafe { p.write(v) };
    Ok(())
}
pub(crate) fn text(p: *const c_char, limit: usize) -> Result<String> {
    checked(p.cast_mut(), 1)?;
    for n in 0..limit {
        // SAFETY: caller supplies a C string through its first NUL or the bounded array extent.
        if unsafe { p.add(n).read() } == 0 {
            return std::str::from_utf8(input(p.cast(), n)?)
                .map(str::to_owned)
                .map_err(|_| INVALID);
        }
    }
    Err(INVALID)
}
pub(crate) fn fixed<const N: usize>(s: &str) -> [c_char; N] {
    let mut out = [0; N];
    for (dst, src) in out[..N - 1].iter_mut().zip(s.bytes()) {
        *dst = src as c_char;
    }
    out
}
pub(crate) fn strncpy(dst: *mut c_char, n: usize, s: &str) -> Result<()> {
    let dst = checked(dst, n)?.as_ptr();
    let used = n.min(s.len());
    // SAFETY: dst spans n writable bytes. The Rust string is independent of the C output.
    unsafe {
        ptr::copy_nonoverlapping(s.as_ptr(), dst.cast(), used);
        ptr::write_bytes(dst.add(used), 0, n - used);
    }
    Ok(())
}
pub(crate) fn records<T: Copy>(items: Vec<T>) -> Result<*mut T> {
    if items.is_empty() {
        return Ok(ptr::null_mut());
    }
    let bytes = items.len().checked_mul(size_of::<T>()).ok_or(FULL)?;
    // SAFETY: malloc is the frozen output allocator. These C records have no destructors.
    let dst = NonNull::new(unsafe { libc::malloc(bytes) }.cast::<T>()).ok_or(INTERNAL)?;
    // SAFETY: dst has room for every initialized item and cannot overlap the Vec.
    unsafe {
        ptr::copy_nonoverlapping(items.as_ptr(), dst.as_ptr(), items.len());
    }
    Ok(dst.as_ptr())
}
