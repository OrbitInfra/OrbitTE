//! Process-local native completion loss: ordinary public crate, real verbs calls.

use std::ffi::{c_char, c_void};

use super::*;

#[derive(Debug, Default)]
#[repr(C)]
pub(super) struct Snapshot {
    pub version: u64,
    pub armed: u64,
    pub phase: u64,
    pub posts: u64,
    pub lost: u64,
    pub errors: u64,
    pub contexts: u64,
    pub sequence: u64,
    pub destroyed_qps: u64,
}

type Command = unsafe extern "C" fn(u32, u32, u32, u32, *mut Snapshot) -> i32;

#[link(name = "dl")]
unsafe extern "C" {
    fn dlsym(handle: *mut c_void, name: *const c_char) -> *mut c_void;
}

pub(super) struct CompletionFault(Command);

impl CompletionFault {
    pub(super) fn load() -> Result<Self> {
        // SAFETY: the supervisor pins this process-local test library by hash.
        // No OrbitTE symbol or production API is replaced by this control ABI.
        let symbol = unsafe { dlsym(std::ptr::null_mut(), c"orbitte_cq_fault".as_ptr()) };
        if symbol.is_null() {
            return Err("missing installed CQ fault library".into());
        }
        // SAFETY: cq_fault.c defines this exact versioned command signature.
        Ok(Self(unsafe {
            std::mem::transmute::<*mut c_void, Command>(symbol)
        }))
    }

    fn command(
        &self,
        command: u32,
        phase: usize,
        operation: u32,
        count: usize,
    ) -> Result<Snapshot> {
        let mut output = Snapshot::default();
        // SAFETY: output is a live, correctly aligned ABI snapshot; arguments are bounded by Case.
        let code = unsafe {
            (self.0)(
                command,
                phase.try_into()?,
                operation,
                count.try_into()?,
                &mut output,
            )
        };
        if code != 0 || output.version != 1 || output.errors != 0 || output.contexts != 2 {
            return Err(format!("invalid native fault receipt ({code}): {output:?}").into());
        }
        Ok(output)
    }

    pub(super) fn arm(&self, case: &Case, phase: usize) -> Result<()> {
        self.command(1, phase, if case.read { 1 } else { 2 }, case.parents)?;
        Ok(())
    }

    pub(super) fn pending(&self, case: &Case, phase: usize) -> Result<Snapshot> {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let receipt = self.command(0, 0, 0, 0)?;
            if receipt.armed != 1 || receipt.phase != phase as u64 {
                return Err("native fault generation changed".into());
            }
            if receipt.posts == case.parents as u64 && receipt.lost == receipt.posts {
                return Ok(receipt);
            }
            if Instant::now() >= deadline {
                return Err(format!("missing native payload CQEs: {receipt:?}").into());
            }
            thread::sleep(Duration::from_millis(1));
        }
    }

    pub(super) fn disarm(&self) -> Result<Snapshot> {
        self.command(2, 0, 0, 0)
    }
}
