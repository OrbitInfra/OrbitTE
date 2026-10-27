//! Public-consumer control ABI for real native dependency failures.

use std::ffi::{CString, c_char, c_void};

use super::*;

#[derive(Debug, Default)]
#[repr(C)]
pub(super) struct Snapshot {
    pub version: u64,
    pub armed: u64,
    pub kind: u64,
    pub attempts: u64,
    pub code: u64,
    pub context: u64,
    pub pd: u64,
    pub cq: u64,
    pub channel: u64,
    pub qpn: u64,
    pub blocker: u64,
    pub live: u64,
    pub sequence: u64,
}
type Command = unsafe extern "C" fn(u32, u32, *const c_char, *mut Snapshot) -> i32;
#[link(name = "dl")]
unsafe extern "C" {
    fn dlsym(handle: *mut c_void, name: *const c_char) -> *mut c_void;
}

pub(super) struct DependencyFault {
    command: Command,
    pub kind: u32,
}
impl DependencyFault {
    pub(super) fn arm(case: &Case, kind: &str) -> Result<Self> {
        let kind = match kind {
            "cq" => 1,
            "channel" if case.events => 2,
            "pd" => 3,
            _ => return Err("invalid native dependency or completion mode".into()),
        };
        // SAFETY: the supervisor pins the observer exporting this versioned ABI.
        let symbol = unsafe { dlsym(std::ptr::null_mut(), c"orbitte_dependency_fault".as_ptr()) };
        if symbol.is_null() {
            return Err("missing native dependency control".into());
        }
        let fault = Self {
            // SAFETY: dependency.c defines this exact signature and snapshot layout.
            command: unsafe { std::mem::transmute::<*mut c_void, Command>(symbol) },
            kind,
        };
        let rail = CString::new(case.rails[usize::from(case.mask == 2)].as_str())?;
        let mut snapshot = Snapshot::default();
        // SAFETY: inputs and the output are live for this synchronous call.
        let code = unsafe { (fault.command)(1, kind, rail.as_ptr(), &mut snapshot) };
        if code != 0 || snapshot.version != 1 || snapshot.armed != 1 || snapshot.attempts != 0 {
            return Err(format!("dependency arm failed ({code}): {snapshot:?}").into());
        }
        Ok(fault)
    }

    pub(super) fn retained(&self) -> Result<Snapshot> {
        let mut snapshot = Snapshot::default();
        // SAFETY: command 2 ignores selectors and writes exactly one ABI snapshot.
        let code = unsafe { (self.command)(2, 0, std::ptr::null(), &mut snapshot) };
        if code != 0
            || snapshot.version != 1
            || snapshot.kind != u64::from(self.kind)
            || snapshot.attempts != 1
            || snapshot.code != 16
            || snapshot.live != 1
            || snapshot.blocker == 0
        {
            return Err(format!("dependency retention failed ({code}): {snapshot:?}").into());
        }
        Ok(snapshot)
    }
}
