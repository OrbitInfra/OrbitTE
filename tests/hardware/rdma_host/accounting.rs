//! Join ordinary public completion counters to a passive native WR/CQE ledger.

use std::ffi::{c_char, c_void};

use super::*;

#[derive(Debug, Default)]
#[repr(C)]
struct Snapshot {
    version: u64,
    active: u64,
    posts: u64,
    completed: u64,
    bytes: u64,
    pending: u64,
    peak: u64,
    rail_peak: [u64; 2],
    sequence: u64,
}

type Command = unsafe extern "C" fn(u32, u32, u32, u32, *mut Snapshot) -> i32;
#[link(name = "dl")]
unsafe extern "C" {
    fn dlsym(handle: *mut c_void, name: *const c_char) -> *mut c_void;
}

pub(super) struct Accounting(Command);

impl Accounting {
    pub(super) fn load(case: &Case, memory: &RegisteredMemory) -> Result<Option<Self>> {
        if env::var("ORBITTE_HOST_MODE")? != "installed-rail-accounting" {
            return Ok(None);
        }
        if case.parents != 16
            || case.payload != (4 << 20) + 1
            || case.block != 1 << 20
            || case.lanes != 1
            || case.mask != 3
        {
            return Err("rail accounting requires the frozen folded-tail profile".into());
        }
        println!(
            "ORBITTE_INSTALLED_RAIL_MEMORY {{\"pid\":{},\"address\":{}}}",
            std::process::id(),
            memory.base_address()
        );
        if case.source {
            return Ok(None);
        }
        // SAFETY: the supervisor pins the observer library and this exact ABI by hash.
        let symbol = unsafe { dlsym(std::ptr::null_mut(), c"orbitte_rail_accounting".as_ptr()) };
        if symbol.is_null() {
            return Err("missing native rail accounting library".into());
        }
        // SAFETY: accounting.c defines Command with this snapshot layout.
        Ok(Some(Self(unsafe {
            std::mem::transmute::<*mut c_void, Command>(symbol)
        })))
    }

    fn command(&self, command: u32, round: usize, case: &Case) -> Result<Snapshot> {
        let mut output = Snapshot::default();
        // SAFETY: output is live/aligned, and the frozen profile bounds all arguments.
        let code = unsafe {
            (self.0)(
                command,
                round.try_into()?,
                if case.read { 1 } else { 2 },
                64,
                &mut output,
            )
        };
        if code != 0 || output.version != 1 {
            return Err(format!("native rail ledger failed ({code}): {output:?}").into());
        }
        Ok(output)
    }

    pub(super) fn begin(&self, round: usize, case: &Case) -> Result<()> {
        self.command(1, round, case)?;
        Ok(())
    }

    pub(super) fn finish(&self, round: usize, case: &Case, engine: &TransferEngine) -> Result<()> {
        let receipt = self.command(2, round, case)?;
        let stats = idle(engine)?;
        if receipt.active != 0
            || receipt.pending != 0
            || receipt.posts != 64
            || receipt.completed != 64
            || receipt.bytes != (case.parents * case.payload) as u64
            || receipt.rail_peak.iter().any(|peak| *peak < 2 || *peak > 8)
            || stats.direct_bytes != 0
            || stats.staging_bytes != 0
            || stats.retained_bytes != 0
        {
            return Err(format!("rail accounting did not drain: {receipt:?}, {stats:?}").into());
        }
        println!(
            "ORBITTE_INSTALLED_RAIL_ROUND {{\"round\":{round},\"posts\":{},\"completed\":{},\"bytes\":{},\"peak\":{},\"rail_peak\":{:?},\"sequence\":{},\"drained\":true}}",
            receipt.posts,
            receipt.completed,
            receipt.bytes,
            receipt.peak,
            receipt.rail_peak,
            receipt.sequence
        );
        Ok(())
    }
}
