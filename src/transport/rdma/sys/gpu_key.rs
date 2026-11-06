//! mlx5 DevX exact-range KLM grants over an already registered local GPU MR.
//! Mailboxes follow the mlx5 PRM (CREATE/QUERY_MKEY and QUERY_HCA_CAP).
use super::*;
use libloading::Library;
use std::sync::OnceLock;

type Open = unsafe extern "C" fn(*mut ffi::ibv_device, *mut OpenAttr) -> *mut ffi::ibv_context;
type General = unsafe extern "C" fn(*mut c_void, *const c_void, usize, *mut c_void, usize) -> c_int;
type Create =
    unsafe extern "C" fn(*mut c_void, *const c_void, usize, *mut c_void, usize) -> *mut c_void;
type Init = unsafe extern "C" fn(*mut c_void, u64) -> c_int;
#[repr(C)]
struct OpenAttr {
    flags: u32,
    comp_mask: u64,
}
struct Driver {
    supports: unsafe extern "C" fn(*mut ffi::ibv_device) -> bool,
    open: Open,
    init: Init,
    general: General,
    create: Create,
    query: General,
    destroy: unsafe extern "C" fn(*mut c_void) -> c_int,
    _library: Library,
}
impl Driver {
    fn load() -> Option<&'static Self> {
        static TABLE: OnceLock<Option<Driver>> = OnceLock::new();
        TABLE
            .get_or_init(|| {
                // SAFETY: use the versioned rdma-core mlx5 ABI. All pointers below are
                // copied from that library and the table keeps the DSO alive.
                unsafe {
                    let lib = Library::new("libmlx5.so.1").ok()?;
                    Some(Self {
                        supports: *lib.get(b"mlx5dv_is_supported\0").ok()?,
                        open: *lib.get(b"mlx5dv_open_device\0").ok()?,
                        init: *lib.get(b"mlx5dv_init_obj\0").ok()?,
                        general: *lib.get(b"mlx5dv_devx_general_cmd\0").ok()?,
                        create: *lib.get(b"mlx5dv_devx_obj_create\0").ok()?,
                        query: *lib.get(b"mlx5dv_devx_obj_query\0").ok()?,
                        destroy: *lib.get(b"mlx5dv_devx_obj_destroy\0").ok()?,
                        _library: lib,
                    })
                }
            })
            .as_ref()
    }
}
unsafe extern "C" {
    fn orbite_mlx5_pdn(init: Init, pd: *mut c_void, pdn: *mut u32) -> c_int;
}

pub(super) unsafe fn open(device: *mut ffi::ibv_device) -> *mut c_void {
    let Some(d) = Driver::load() else {
        return std::ptr::null_mut();
    };
    // SAFETY: the caller retains the device list. Request DevX only on a supported device.
    unsafe {
        if !(d.supports)(device) {
            return std::ptr::null_mut();
        }
        (d.open)(
            device,
            &mut OpenAttr {
                flags: 1,
                comp_mask: 0,
            },
        )
        .cast()
    }
}

fn word(b: &[u8], i: usize) -> u32 {
    u32::from_be_bytes([b[i], b[i + 1], b[i + 2], b[i + 3]])
}
fn set(b: &mut [u8], i: usize, v: u32) {
    b[i..i + 4].copy_from_slice(&v.to_be_bytes());
}

pub(super) unsafe fn caps(h: &DeviceHandles) -> GpuKeyRange {
    let mut caps = GpuKeyRange::default();
    let Some(d) = Driver::load() else {
        return caps;
    };
    let mut input = [0u8; 16];
    input[..2].copy_from_slice(&0x100u16.to_be_bytes());
    let mut query = |page: u16| {
        let mut out = [0u8; 16 + 4096];
        input[6..8].copy_from_slice(&((page << 1) | 1).to_be_bytes());
        // SAFETY: both mailboxes cover the fixed firmware command and the context is live.
        let code = unsafe {
            (d.general)(
                h.context,
                input.as_ptr().cast(),
                input.len(),
                out.as_mut_ptr().cast(),
                out.len(),
            )
        };
        (code == 0 && out[0] == 0).then_some(out)
    };
    // IMAICL is required for remote rights over a local-only child key.
    // A named index lets the PD burn the full rkey *before* making it live.
    if query(0).is_none_or(|page| page[16 + 0x231 / 8] & (0x80 >> (0x231 % 8)) == 0) {
        return caps;
    }
    let Some(page) = query(0x20) else { return caps };
    let reserve = word(&page, 16 + 0x3c0 / 8);
    let base = reserve & 0xff_ffff;
    let count = 1u32.checked_shl((reserve >> 24) & 0x3f).unwrap_or(0);
    if reserve >> 31 == 0
        || base == 0
        || count == 0
        || base.checked_add(count).is_none_or(|n| n > 1 << 24)
    {
        return caps;
    }
    // SAFETY: the optional header shim supplies the provider object layout for this PD.
    if unsafe { orbite_mlx5_pdn(d.init, h.pd, &mut caps.pdn) } == 0 && caps.pdn <= 0xff_ffff {
        caps.base = base;
        caps.count = count;
    }
    caps
}

const MKC: usize = 16;
const KLM: usize = 0x880 / 8;
const ACCESS_MASK: u32 = 0x5c_00_ff_00; // free, atomic, read/write and access mode

fn create_command(
    pdn: u32,
    lkey: u32,
    qpn: u32,
    addr: u64,
    length: u32,
    permission: u32,
    rkey: u32,
) -> [u8; KLM + 64] {
    let mut inbox = [0u8; KLM + 64];
    inbox[..2].copy_from_slice(&0x200u16.to_be_bytes());
    set(&mut inbox, 8, rkey >> 8);
    // CREATE an indirect KLM with local read and the requested remote right.
    // Type-2 remote invalidation is enabled in the PD word below.
    let flags = (1 << 15)
        | if permission == 1 {
            1 << 12
        } else {
            (1 << 13) | (1 << 11)
        }
        | (1 << 10)
        | (2 << 8);
    set(&mut inbox, MKC, flags);
    set(&mut inbox, MKC + 4, (qpn << 8) | (rkey & 255));
    set(&mut inbox, MKC + 12, (1 << 24) | pdn);
    inbox[MKC + 16..MKC + 24].copy_from_slice(&addr.to_be_bytes());
    inbox[MKC + 24..MKC + 32].copy_from_slice(&u64::from(length).to_be_bytes());
    set(&mut inbox, MKC + 52, 4); // padded KLM table, one nonempty entry
    set(&mut inbox, 0x300 / 8, 1); // one supplied translation octword
    set(&mut inbox, KLM, length);
    set(&mut inbox, KLM + 4, lkey);
    inbox[KLM + 8..KLM + 16].copy_from_slice(&addr.to_be_bytes());
    inbox
}

fn confirms_grant(input: &[u8; KLM + 64], output: &[u8; KLM + 32]) -> bool {
    output[0] == 0
        && word(output, MKC) & ACCESS_MASK == word(input, MKC) & ACCESS_MASK
        && output[MKC + 4..MKC + 8] == input[MKC + 4..MKC + 8]
        && word(output, MKC + 12) & 0x81ff_ffff == word(input, MKC + 12)
        && output[MKC + 16..MKC + 32] == input[MKC + 16..MKC + 32]
        && output[KLM..KLM + 16] == input[KLM..KLM + 16]
}

pub(super) unsafe extern "C" fn create(
    h: *const DeviceHandles,
    mr: *mut MemoryHandles,
    qpn: u32,
    addr: u64,
    length: u32,
    permission: u32,
    rkey: u32,
    probe: bool,
) -> c_int {
    let Some(d) = Driver::load() else {
        return libc::EOPNOTSUPP;
    };
    // SAFETY: the calling grant or registration owns the MR and context through destroy.
    let (h, m) = unsafe { (&*h, &mut *mr) };
    if m.mr.is_null()
        || !m.gpu_key.is_null()
        || length == 0
        || length > 1 << 31
        || qpn >= 0xff_ffff
        || qpn == 0
        || (qpn == GPU_PROBE_QPN) != probe
        || !matches!(permission, 1 | 2)
    {
        return libc::EINVAL;
    }
    // SAFETY: this is the active, local-only parent MR in this exact device PD.
    let parent = unsafe { &*m.mr.cast::<ffi::ibv_mr>() };
    if parent.pd.cast() != h.pd
        || addr
            .checked_sub(parent.addr as u64)
            .and_then(|n| n.checked_add(length.into()))
            .is_none_or(|end| end > parent.length as u64)
    {
        return libc::EINVAL;
    }
    let inbox = create_command(
        h.gpu_keys.pdn,
        parent.lkey,
        qpn,
        addr,
        length,
        permission,
        rkey,
    );
    let mut created = [0u8; 16];
    // SAFETY: fixed CREATE_MKEY mailboxes. Keep even a partially verified object in its owner.
    m.gpu_key = unsafe {
        (d.create)(
            h.context,
            inbox.as_ptr().cast(),
            inbox.len(),
            created.as_mut_ptr().cast(),
            created.len(),
        )
    };
    if m.gpu_key.is_null() {
        return if created[0] != 0 {
            libc::EOPNOTSUPP
        } else {
            failure()
        };
    }
    if created[0] != 0 || word(&created, 8) & 0xff_ffff != rkey >> 8 {
        return libc::EPROTO;
    }
    let mut query = [0u8; 16];
    query[..2].copy_from_slice(&0x201u16.to_be_bytes());
    set(&mut query, 8, rkey >> 8);
    let mut out = [0u8; KLM + 32];
    // SAFETY: read back this exact owned object. Unknown, wildcard or widened contexts fail closed.
    let code = unsafe {
        (d.query)(
            m.gpu_key,
            query.as_ptr().cast(),
            query.len(),
            out.as_mut_ptr().cast(),
            out.len(),
        )
    };
    if code != 0 {
        return code;
    }
    if confirms_grant(&inbox, &out) {
        0
    } else {
        libc::EOPNOTSUPP
    }
}

pub(super) unsafe extern "C" fn destroy(key: *mut c_void) -> c_int {
    let Some(d) = Driver::load() else {
        return libc::ENOSYS;
    };
    // SAFETY: the owner supplies its live DevX object exactly once.
    unsafe { (d.destroy)(key) }
}

#[cfg(test)]
#[path = "../../../../tests/unit/transport/rdma/gpu_key_mailbox.rs"]
mod tests;
