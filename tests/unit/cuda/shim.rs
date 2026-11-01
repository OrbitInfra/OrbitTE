use std::cell::{RefCell, UnsafeCell};
use std::collections::BTreeMap;
use std::ffi::{c_int, c_uint, c_void};
use std::sync::atomic::AtomicI32;
use std::sync::{OnceLock, Weak};

use super::*;
use driver::Status;

// The shim owns actual host backing and records Driver API boundaries. These
// tests prove CPU ownership contracts, not GPU execution or memory ordering.
type Gate = Option<(
    std::sync::mpsc::SyncSender<()>,
    std::sync::mpsc::Receiver<()>,
)>;

pub(super) struct Device {
    pub(super) copy_gate: Mutex<Gate>,
    pub(super) event_gate: Mutex<
        Option<(
            std::sync::mpsc::SyncSender<()>,
            std::sync::mpsc::Receiver<()>,
        )>,
    >,
    pub(super) bytes: UnsafeCell<Box<[u8]>>,
    pub(super) pins: Mutex<BTreeMap<usize, Box<[u8]>>>,
    pub(super) host_registrations: Mutex<BTreeMap<usize, usize>>,
    pub(super) ordinal: AtomicI32,
    pub(super) pci_device: AtomicI32,
    pub(super) streams: Mutex<BTreeMap<usize, Vec<PendingCopy>>>,
    pub(super) submitted_copies: Mutex<Vec<(usize, usize, usize)>>,
    pub(super) completions: Mutex<BTreeMap<usize, Option<usize>>>,
    next_handle: AtomicUsize,
    pub(super) imports: Mutex<BTreeMap<u64, usize>>,
    pub(super) ipc_copies: AtomicUsize,
    pub(super) ipc_copy_fail_at: AtomicUsize,
    pub(super) ipc_wait_fail_at: AtomicUsize,
    pub(super) ipc_pop_fail_at: AtomicUsize,
    pub(super) ipc_open_error: AtomicI32,
    pub(super) ipc_close_error: AtomicI32,
    pub(super) ipc_partial_open: AtomicBool,
    pub(super) ipc_pop_after_open: AtomicBool,
    pub(super) ipc_pop_after_close: AtomicBool,
    pub(super) stream_create_error: AtomicI32,
    pub(super) stream_partial_create: AtomicBool,
    pub(super) event_gate_skip: AtomicUsize,
    pub(super) event_waits: AtomicUsize,
    pub(super) event_queries: AtomicUsize,
    pub(super) event_not_ready: AtomicUsize,
    pub(super) event_query_results: AtomicUsize,
    pub(super) event_query_successes: AtomicUsize,
    pub(super) event_query_fail_at: AtomicUsize,
    pub(super) query_hold: AtomicBool,
    pub(super) query_hold_address: AtomicUsize,
    pub(super) pop_after_not_ready: AtomicBool,
    pub(super) event_wait_fail_at: AtomicUsize,
    pub(super) event_create_error: AtomicI32,
    pub(super) record_error: AtomicI32,
    pub(super) stream_destroy_error: AtomicI32,
    pub(super) event_destroy_error: AtomicI32,
    pub(super) copy_error: AtomicI32,
    pub(super) sync_error: AtomicI32,
    #[cfg(feature = "rdma")]
    pub(super) rdma_ordering: AtomicI32,
    #[cfg(feature = "rdma")]
    pub(super) rdma_flush_options: AtomicI32,
    #[cfg(feature = "rdma")]
    pub(super) rdma_set_error: AtomicI32,
    #[cfg(feature = "rdma")]
    pub(super) rdma_flush_error: AtomicI32,
    pub(super) pop_error: AtomicI32,
    pub(super) pop_after_completion: AtomicBool,
    pub(super) free_error: AtomicI32,
    pub(super) pin_error: AtomicI32,
    pub(super) pop_after_pin: AtomicBool,
    pub(super) pop_after_unpin: AtomicBool,
    pub(super) memory_type: AtomicI32,
    pub(super) managed: AtomicI32,
    pub(super) events: Mutex<Vec<&'static str>>,
}
pub(super) struct PendingCopy {
    pub(super) source: usize,
    pub(super) destination: usize,
    length: usize,
}

// SAFETY: CudaMemory access locks and exclusive range loans exclude conflicting
// device-byte access. Pinned buffers are private; maps serialize their lifecycle.
unsafe impl Send for Device {}
unsafe impl Sync for Device {}
thread_local! { pub(super) static STACK: RefCell<Vec<usize>> = const { RefCell::new(Vec::new()) }; }
static DEVICES: OnceLock<Mutex<BTreeMap<u64, Weak<Device>>>> = OnceLock::new();
fn device_at(address: u64) -> Arc<Device> {
    DEVICES
        .get()
        .unwrap()
        .lock()
        .unwrap()
        .range(..=address)
        .next_back()
        .unwrap()
        .1
        .upgrade()
        .unwrap()
}
fn current() -> &'static Device {
    let pointer = STACK.with(|stack| *stack.borrow().last().unwrap());
    // The Context retains its actual owner until a successful pop, or forever
    // on an unproven restore. The test shim never exposes this reference publicly.
    unsafe { &*(pointer as *const Device) }
}
fn event(name: &'static str) {
    current().events.lock().unwrap().push(name);
}
fn status(code: c_int) -> Status {
    match code {
        0 => SUCCESS,
        1 => Status::CUDA_ERROR_INVALID_VALUE,
        201 => Status::CUDA_ERROR_INVALID_CONTEXT,
        700 => Status::CUDA_ERROR_ILLEGAL_ADDRESS,
        _ => panic!("unexpected CUDA test error {code}"),
    }
}
unsafe extern "C" fn init(_: c_uint) -> Status {
    SUCCESS
}
unsafe extern "C" fn get(output: *mut c_int, ordinal: c_int) -> Status {
    unsafe {
        *output = ordinal;
    }
    SUCCESS
}
unsafe extern "C" fn retain(_: *mut driver::Context, _: c_int) -> Status {
    unreachable!()
}
unsafe extern "C" fn release(_: c_int) -> Status {
    unreachable!()
}
unsafe extern "C" fn push(context: driver::Context) -> Status {
    STACK.with(|s| s.borrow_mut().push(context as usize));
    event("push");
    SUCCESS
}
unsafe extern "C" fn pop(output: *mut driver::Context) -> Status {
    event("pop");
    let error = current().pop_error.load(Ordering::Acquire);
    if error != 0 {
        return status(error);
    }
    unsafe {
        *output = STACK.with(|s| s.borrow_mut().pop().unwrap()) as driver::Context;
    }
    SUCCESS
}
unsafe extern "C" fn sync() -> Status {
    event("sync");
    status(current().sync_error.load(Ordering::Acquire))
}
unsafe extern "C" fn allocate(_: *mut u64, _: usize) -> Status {
    unreachable!()
}
unsafe extern "C" fn free(_: u64) -> Status {
    event("free");
    status(current().free_error.load(Ordering::Acquire))
}
unsafe extern "C" fn memset(_: u64, _: u8, _: usize) -> Status {
    unreachable!()
}
unsafe extern "C" fn stream_create(output: *mut sys::CUstream, flags: c_uint) -> Status {
    event("stream-create");
    assert_eq!(flags, sys::CUstream_flags::CU_STREAM_NON_BLOCKING as u32);
    let error = current().stream_create_error.load(Ordering::Acquire);
    if error == 0 || current().stream_partial_create.load(Ordering::Acquire) {
        let handle = current().next_handle.fetch_add(1, Ordering::Relaxed);
        current().streams.lock().unwrap().insert(handle, Vec::new());
        unsafe {
            *output = handle as sys::CUstream;
        }
    }
    status(error)
}
unsafe extern "C" fn event_create(output: *mut sys::CUevent, flags: c_uint) -> Status {
    event("event-create");
    assert_eq!(
        flags,
        sys::CUevent_flags::CU_EVENT_DISABLE_TIMING as u32
            | sys::CUevent_flags::CU_EVENT_BLOCKING_SYNC as u32
    );
    let error = current().event_create_error.load(Ordering::Acquire);
    if error == 0 {
        let handle = current().next_handle.fetch_add(1, Ordering::Relaxed);
        current().completions.lock().unwrap().insert(handle, None);
        unsafe {
            *output = handle as sys::CUevent;
        }
    }
    status(error)
}
unsafe extern "C" fn stream_destroy(stream: sys::CUstream) -> Status {
    event("stream-destroy");
    let error = current().stream_destroy_error.load(Ordering::Acquire);
    if error == 0 {
        let pending = current()
            .streams
            .lock()
            .unwrap()
            .remove(&(stream as usize))
            .unwrap();
        assert!(pending.is_empty(), "destroy is not a DMA drain receipt");
    }
    status(error)
}
unsafe extern "C" fn event_destroy(completion: sys::CUevent) -> Status {
    event("event-destroy");
    let error = current().event_destroy_error.load(Ordering::Acquire);
    if error == 0 {
        assert!(
            current()
                .completions
                .lock()
                .unwrap()
                .remove(&(completion as usize))
                .is_some()
        );
    }
    status(error)
}
unsafe extern "C" fn event_record(completion: sys::CUevent, stream: sys::CUstream) -> Status {
    event("record");
    let error = current().record_error.load(Ordering::Acquire);
    if error == 0 {
        *current()
            .completions
            .lock()
            .unwrap()
            .get_mut(&(completion as usize))
            .unwrap() = Some(stream as usize);
    }
    status(error)
}
fn drain(stream: usize) -> Status {
    let error = current().sync_error.load(Ordering::Acquire);
    if error == 0 {
        for copy in current()
            .streams
            .lock()
            .unwrap()
            .get_mut(&stream)
            .unwrap()
            .drain(..)
        {
            // Complete only here, never at enqueue: early staging release/read
            // would leave these pointers dangling or expose unchanged bytes.
            unsafe {
                std::ptr::copy_nonoverlapping(
                    copy.source as *const u8,
                    copy.destination as *mut u8,
                    copy.length,
                );
            }
        }
    }
    status(error)
}
unsafe extern "C" fn stream_synchronize(stream: sys::CUstream) -> Status {
    event("stream-sync");
    drain(stream as usize)
}
fn copy_held(device: &Device, stream: usize) -> bool {
    if device.query_hold.load(Ordering::Acquire) {
        return true;
    }
    let address = device.query_hold_address.load(Ordering::Acquire);
    address != 0
        && device.streams.lock().unwrap()[&stream]
            .iter()
            .any(|copy| copy.source == address || copy.destination == address)
}

unsafe extern "C" fn event_query(completion: sys::CUevent) -> Status {
    event("event-query");
    let device = current();
    device.event_queries.fetch_add(1, Ordering::AcqRel);
    let stream = device.completions.lock().unwrap()[&(completion as usize)]
        .expect("unrecorded event cannot prove this copy complete");
    if copy_held(device, stream) {
        device.event_not_ready.fetch_add(1, Ordering::AcqRel);
        if device.pop_after_not_ready.load(Ordering::Acquire) {
            device.pop_error.store(201, Ordering::Release);
        }
        return Status::CUDA_ERROR_NOT_READY;
    }
    let ordinal = device.event_query_results.fetch_add(1, Ordering::AcqRel) + 1;
    let result = if ordinal == device.event_query_fail_at.load(Ordering::Acquire) {
        status(700)
    } else {
        drain(stream)
    };
    if result == SUCCESS {
        device.event_query_successes.fetch_add(1, Ordering::AcqRel);
    }
    if device.pop_after_completion.load(Ordering::Acquire) {
        device.pop_error.store(201, Ordering::Release);
    }
    result
}

unsafe extern "C" fn event_synchronize(completion: sys::CUevent) -> Status {
    event("event-sync");
    let device = current();
    let gate = {
        let mut gate = device.event_gate.lock().unwrap();
        if device.event_gate_skip.load(Ordering::Acquire) == 0 {
            gate.take()
        } else {
            device.event_gate_skip.fetch_sub(1, Ordering::AcqRel);
            None
        }
    };
    if let Some((entered, resume)) = gate {
        if entered.send(()).is_err() || resume.recv_timeout(Duration::from_secs(3)).is_err() {
            return Status::CUDA_ERROR_INVALID_VALUE;
        }
    }
    let stream = current().completions.lock().unwrap()[&(completion as usize)]
        .expect("unrecorded event cannot prove this copy complete");
    let until = std::time::Instant::now() + Duration::from_secs(3);
    while copy_held(device, stream) {
        if std::time::Instant::now() >= until {
            return Status::CUDA_ERROR_INVALID_VALUE;
        }
        std::thread::sleep(Duration::from_micros(50));
    }
    let ordinal = current().event_waits.fetch_add(1, Ordering::AcqRel) + 1;
    let result = if ordinal == current().event_wait_fail_at.load(Ordering::Acquire) {
        status(700)
    } else {
        drain(stream)
    };
    if current().pop_after_completion.load(Ordering::Acquire) {
        current().pop_error.store(201, Ordering::Release);
    }
    result
}
fn enqueue(stream: sys::CUstream, source: usize, destination: usize, length: usize) {
    current()
        .submitted_copies
        .lock()
        .unwrap()
        .push((source, destination, length));
    current()
        .streams
        .lock()
        .unwrap()
        .get_mut(&(stream as usize))
        .unwrap()
        .push(PendingCopy {
            source,
            destination,
            length,
        });
}
fn enter_copy_gate() -> bool {
    let gate = current().copy_gate.lock().unwrap().take();
    gate.is_none_or(|(entered, resume)| {
        entered.send(()).is_ok() && resume.recv_timeout(Duration::from_secs(3)).is_ok()
    })
}

unsafe extern "C" fn to_host(
    output: *mut c_void,
    address: u64,
    length: usize,
    stream: sys::CUstream,
) -> Status {
    event("DtoH");
    if !enter_copy_gate() {
        return Status::CUDA_ERROR_INVALID_VALUE;
    }
    if !current().imports.lock().unwrap().is_empty() {
        ipc_copy_control();
    }
    let error = current().copy_error.load(Ordering::Acquire);
    enqueue(
        stream,
        address as usize,
        output as usize,
        if error == 0 { length } else { 0 },
    );
    status(error)
}
unsafe extern "C" fn to_device(
    address: u64,
    input: *const c_void,
    length: usize,
    stream: sys::CUstream,
) -> Status {
    event("HtoD");
    if !enter_copy_gate() {
        return Status::CUDA_ERROR_INVALID_VALUE;
    }
    if !current().imports.lock().unwrap().is_empty() {
        ipc_copy_control();
    }
    let error = current().copy_error.load(Ordering::Acquire);
    // Even a failed enqueue may leave a prefix scheduled. Drain is not rollback.
    enqueue(
        stream,
        input as usize,
        address as usize,
        if error == 0 { length } else { length.min(1) },
    );
    status(error)
}
unsafe extern "C" fn host_allocate(output: *mut *mut c_void, length: usize, _: c_uint) -> Status {
    event("pin");
    let mut bytes = vec![0; length].into_boxed_slice();
    let pointer = bytes.as_mut_ptr();
    current()
        .pins
        .lock()
        .unwrap()
        .insert(pointer as usize, bytes);
    unsafe {
        *output = pointer.cast();
    }
    if current().pop_after_pin.load(Ordering::Acquire) {
        current().pop_error.store(201, Ordering::Release);
    }
    status(current().pin_error.load(Ordering::Acquire))
}
unsafe extern "C" fn host_free(pointer: *mut c_void) -> Status {
    event("unpin");
    assert!(
        current()
            .streams
            .lock()
            .unwrap()
            .values()
            .flatten()
            .all(|copy| copy.source != pointer as usize && copy.destination != pointer as usize)
    );
    let error = current().free_error.load(Ordering::Acquire);
    if error == 0 {
        assert!(
            current()
                .pins
                .lock()
                .unwrap()
                .remove(&(pointer as usize))
                .is_some()
        );
    }
    if current().pop_after_unpin.load(Ordering::Acquire) {
        current().pop_error.store(201, Ordering::Release);
    }
    status(error)
}
unsafe extern "C" fn attribute(
    output: *mut c_void,
    attribute: PointerAttribute,
    address: u64,
) -> Status {
    let device = device_at(address);
    unsafe {
        match attribute {
            PointerAttribute::CU_POINTER_ATTRIBUTE_CONTEXT => {
                *output.cast::<driver::Context>() = Arc::as_ptr(&device) as driver::Context
            }
            PointerAttribute::CU_POINTER_ATTRIBUTE_MEMORY_TYPE => {
                *output.cast::<i32>() = device.memory_type.load(Ordering::Acquire)
            }
            PointerAttribute::CU_POINTER_ATTRIBUTE_IS_MANAGED => {
                *output.cast::<u32>() = device.managed.load(Ordering::Acquire) as u32
            }
            PointerAttribute::CU_POINTER_ATTRIBUTE_DEVICE_ORDINAL => {
                *output.cast::<i32>() = device.ordinal.load(Ordering::Acquire)
            }
            _ => return Status::CUDA_ERROR_INVALID_VALUE,
        }
    }
    SUCCESS
}
unsafe extern "C" fn range(base: *mut u64, length: *mut usize, address: u64) -> Status {
    let device = device_at(address);
    let bytes = unsafe { &*device.bytes.get() };
    unsafe {
        *base = bytes.as_ptr() as u64;
        *length = bytes.len();
    }
    SUCCESS
}
pub(super) fn driver() -> Arc<Driver> {
    Arc::new(Driver {
        _library: None,
        init,
        device_get: get,
        #[cfg(feature = "rdma")]
        device_attribute,
        #[cfg(feature = "rdma")]
        set_pointer_attribute,
        #[cfg(feature = "rdma")]
        flush_rdma,
        primary_retain: retain,
        primary_release: release,
        push,
        pop,
        synchronize: sync,
        allocate,
        free,
        memset,
        ipc_export,
        ipc_open,
        ipc_close,
        copy_batch: None,
        copy_device,
        to_host,
        to_device,
        stream_create,
        stream_destroy,
        stream_synchronize,
        event_create,
        event_destroy,
        event_record,
        event_synchronize,
        event_query,
        host_allocate,
        host_free,
        host_register,
        host_unregister,
        pci_bus_id,
        pointer_attribute: attribute,
        address_range: range,
    })
}
pub(super) fn allocation(bytes: &[u8]) -> Arc<Device> {
    let device = Arc::new(Device {
        copy_gate: Mutex::new(None),
        event_gate: Mutex::new(None),
        bytes: UnsafeCell::new(bytes.to_vec().into_boxed_slice()),
        pins: Mutex::default(),
        host_registrations: Mutex::default(),
        ordinal: AtomicI32::new(0),
        pci_device: AtomicI32::new(-1),
        streams: Mutex::default(),
        submitted_copies: Mutex::default(),
        completions: Mutex::default(),
        next_handle: AtomicUsize::new(1),
        imports: Mutex::default(),
        ipc_copies: AtomicUsize::new(0),
        ipc_copy_fail_at: AtomicUsize::new(0),
        ipc_wait_fail_at: AtomicUsize::new(0),
        ipc_pop_fail_at: AtomicUsize::new(0),
        ipc_open_error: AtomicI32::new(0),
        ipc_close_error: AtomicI32::new(0),
        ipc_partial_open: AtomicBool::new(false),
        ipc_pop_after_open: AtomicBool::new(false),
        ipc_pop_after_close: AtomicBool::new(false),
        stream_create_error: AtomicI32::new(0),
        stream_partial_create: AtomicBool::new(false),
        event_gate_skip: AtomicUsize::new(0),
        event_waits: AtomicUsize::new(0),
        event_queries: AtomicUsize::new(0),
        event_not_ready: AtomicUsize::new(0),
        event_query_results: AtomicUsize::new(0),
        event_query_successes: AtomicUsize::new(0),
        event_query_fail_at: AtomicUsize::new(0),
        query_hold: AtomicBool::new(false),
        query_hold_address: AtomicUsize::new(0),
        pop_after_not_ready: AtomicBool::new(false),
        event_wait_fail_at: AtomicUsize::new(0),
        event_create_error: AtomicI32::new(0),
        record_error: AtomicI32::new(0),
        stream_destroy_error: AtomicI32::new(0),
        event_destroy_error: AtomicI32::new(0),
        copy_error: AtomicI32::new(0),
        sync_error: AtomicI32::new(0),
        #[cfg(feature = "rdma")]
        rdma_ordering: AtomicI32::new(0),
        #[cfg(feature = "rdma")]
        rdma_flush_options: AtomicI32::new(1),
        #[cfg(feature = "rdma")]
        rdma_set_error: AtomicI32::new(0),
        #[cfg(feature = "rdma")]
        rdma_flush_error: AtomicI32::new(0),
        pop_error: AtomicI32::new(0),
        pop_after_completion: AtomicBool::new(false),
        free_error: AtomicI32::new(0),
        pin_error: AtomicI32::new(0),
        pop_after_pin: AtomicBool::new(false),
        pop_after_unpin: AtomicBool::new(false),
        memory_type: AtomicI32::new(2),
        managed: AtomicI32::new(0),
        events: Mutex::default(),
    });
    let address = unsafe { &*device.bytes.get() }.as_ptr() as u64;
    DEVICES
        .get_or_init(Mutex::default)
        .lock()
        .unwrap()
        .insert(address, Arc::downgrade(&device));
    device
}
pub(super) fn memory(device: &Arc<Device>) -> Arc<CudaMemory> {
    let bytes = unsafe { &*device.bytes.get() };
    Arc::new(
        CudaMemory::register(bytes.as_ptr() as u64, bytes.len(), device.clone(), driver()).unwrap(),
    )
}

#[cfg(feature = "rdma")]
unsafe extern "C" fn device_attribute(
    output: *mut i32,
    attribute: sys::CUdevice_attribute,
    _: i32,
) -> Status {
    let device = current();
    let value = match attribute {
        sys::CUdevice_attribute::CU_DEVICE_ATTRIBUTE_GPU_DIRECT_RDMA_WRITES_ORDERING => {
            device.rdma_ordering.load(Ordering::Acquire)
        }
        sys::CUdevice_attribute::CU_DEVICE_ATTRIBUTE_GPU_DIRECT_RDMA_FLUSH_WRITES_OPTIONS => {
            device.rdma_flush_options.load(Ordering::Acquire)
        }
        _ => return Status::CUDA_ERROR_INVALID_VALUE,
    };
    unsafe {
        *output = value;
    }
    SUCCESS
}
#[cfg(feature = "rdma")]
unsafe extern "C" fn set_pointer_attribute(
    value: *const c_void,
    attribute: PointerAttribute,
    address: u64,
) -> Status {
    assert_eq!(
        attribute,
        PointerAttribute::CU_POINTER_ATTRIBUTE_SYNC_MEMOPS
    );
    assert_eq!(unsafe { *value.cast::<u32>() }, 1);
    let device = device_at(address);
    assert!(std::ptr::eq(current(), Arc::as_ptr(&device)));
    event("sync_memops");
    status(device.rdma_set_error.load(Ordering::Acquire))
}
#[cfg(feature = "rdma")]
unsafe extern "C" fn flush_rdma(
    target: sys::CUflushGPUDirectRDMAWritesTarget,
    scope: sys::CUflushGPUDirectRDMAWritesScope,
) -> Status {
    assert_eq!(
        target,
        sys::CUflushGPUDirectRDMAWritesTarget::CU_FLUSH_GPU_DIRECT_RDMA_WRITES_TARGET_CURRENT_CTX
    );
    assert_eq!(
        scope,
        sys::CUflushGPUDirectRDMAWritesScope::CU_FLUSH_GPU_DIRECT_RDMA_WRITES_TO_OWNER
    );
    event("flush_rdma");
    status(current().rdma_flush_error.load(Ordering::Acquire))
}

unsafe extern "C" fn ipc_export(output: *mut sys::CUipcMemHandle, address: u64) -> Status {
    event("ipc-export");
    let mut bytes = [0u8; 64];
    bytes[..8].copy_from_slice(&address.to_ne_bytes());
    unsafe {
        *output = sys::CUipcMemHandle {
            reserved: bytes.map(|b| b as _),
        };
    }
    SUCCESS
}

unsafe extern "C" fn ipc_open(
    output: *mut u64,
    handle: sys::CUipcMemHandle,
    flags: c_uint,
) -> Status {
    event("ipc-open");
    assert_eq!(
        flags,
        sys::CUipcMem_flags::CU_IPC_MEM_LAZY_ENABLE_PEER_ACCESS as u32
    );
    let mut bytes = [0u8; 8];
    for (out, byte) in bytes.iter_mut().zip(handle.reserved) {
        *out = byte as u8;
    }
    let address = u64::from_ne_bytes(bytes);
    let error = current().ipc_open_error.load(Ordering::Acquire);
    if error == 0 || current().ipc_partial_open.load(Ordering::Acquire) {
        assert!(
            DEVICES
                .get()
                .unwrap()
                .lock()
                .unwrap()
                .get(&address)
                .and_then(Weak::upgrade)
                .is_some()
        );
        *current()
            .imports
            .lock()
            .unwrap()
            .entry(address)
            .or_default() += 1;
        unsafe {
            *output = address;
        }
    }
    if current().ipc_pop_after_open.load(Ordering::Acquire) {
        current().pop_error.store(201, Ordering::Release);
    }
    status(error)
}

unsafe extern "C" fn ipc_close(address: u64) -> Status {
    event("ipc-close");
    assert!(
        current()
            .streams
            .lock()
            .unwrap()
            .values()
            .all(Vec::is_empty),
        "close is not a DMA receipt"
    );
    let error = current().ipc_close_error.load(Ordering::Acquire);
    if error == 0 {
        let mut imports = current().imports.lock().unwrap();
        let count = imports.get_mut(&address).unwrap();
        *count -= 1;
        if *count == 0 {
            imports.remove(&address);
        }
        if current().ipc_pop_after_close.load(Ordering::Acquire) {
            current().pop_error.store(201, Ordering::Release);
        }
    }
    status(error)
}

unsafe extern "C" fn copy_device(
    destination: u64,
    source: u64,
    length: usize,
    stream: sys::CUstream,
) -> Status {
    event("DtoD");
    ipc_copy_control();
    let error = current().copy_error.load(Ordering::Acquire);
    enqueue(
        stream,
        source as usize,
        destination as usize,
        if error == 0 { length } else { length.min(1) },
    );
    status(error)
}

pub(super) unsafe extern "C" fn copy_batch(
    destinations: *mut u64,
    sources: *mut u64,
    lengths: *mut usize,
    count: usize,
    attrs: *mut sys::CUmemcpyAttributes,
    indices: *mut usize,
    num_attrs: usize,
    stream: sys::CUstream,
) -> Status {
    event("copy-batch");
    assert!(count > 0 && num_attrs == 1);
    unsafe {
        assert_eq!(*indices, 0);
        assert_eq!(
            (*attrs).srcAccessOrder,
            sys::CUmemcpySrcAccessOrder::CU_MEMCPY_SRC_ACCESS_ORDER_STREAM
        );
        assert_eq!((*attrs).flags, 0);
        // Deliberately enqueue in reverse order; successful batches must not
        // depend on request order. An error can follow a non-prefix subset.
        for index in (0..count).rev() {
            ipc_copy_control();
            enqueue(
                stream,
                *sources.add(index) as usize,
                *destinations.add(index) as usize,
                *lengths.add(index),
            );
            let error = current().copy_error.load(Ordering::Acquire);
            if error != 0 {
                return status(error);
            }
        }
    }
    SUCCESS
}

fn ipc_copy_control() {
    let count = current().ipc_copies.fetch_add(1, Ordering::AcqRel) + 1;
    if count == current().ipc_copy_fail_at.load(Ordering::Acquire) {
        current().copy_error.store(1, Ordering::Release);
    }
    if count == current().ipc_wait_fail_at.load(Ordering::Acquire) {
        current().sync_error.store(700, Ordering::Release);
    }
    if count == current().ipc_pop_fail_at.load(Ordering::Acquire) {
        current()
            .pop_after_completion
            .store(true, Ordering::Release);
    }
}

pub(super) fn wait_pending(device: &Device, count: usize) {
    let until = std::time::Instant::now() + Duration::from_secs(3);
    loop {
        let pending = device
            .streams
            .lock()
            .unwrap()
            .values()
            .filter(|copy| !copy.is_empty())
            .count();
        if pending == count && device.event_queries.load(Ordering::Acquire) != 0 {
            return;
        }
        assert!(
            std::time::Instant::now() < until,
            "pending CUDA copies: {pending}, expected {count}"
        );
        std::thread::yield_now();
    }
}

unsafe extern "C" fn host_register(pointer: *mut c_void, length: usize, flags: c_uint) -> Status {
    assert_eq!(flags, 1);
    assert_eq!(
        pointer as usize % crate::platform::numa::page_round(1).unwrap(),
        0
    );
    event("host-register");
    assert!(
        current()
            .host_registrations
            .lock()
            .unwrap()
            .insert(pointer as usize, length)
            .is_none()
    );
    if current().pop_after_pin.load(Ordering::Acquire) {
        current().pop_error.store(201, Ordering::Release);
    }
    status(current().pin_error.load(Ordering::Acquire))
}
unsafe extern "C" fn host_unregister(pointer: *mut c_void) -> Status {
    event("host-unregister");
    let mut resident = 0u8;
    assert_eq!(
        unsafe {
            libc::mincore(
                pointer,
                crate::platform::numa::page_round(1).unwrap(),
                &mut resident,
            )
        },
        0,
        "unregister lost its actual mapped backing"
    );
    assert!(
        current()
            .streams
            .lock()
            .unwrap()
            .values()
            .flatten()
            .all(|copy| copy.source != pointer as usize && copy.destination != pointer as usize)
    );
    let error = current().free_error.load(Ordering::Acquire);
    if error == 0 {
        assert!(
            current()
                .host_registrations
                .lock()
                .unwrap()
                .remove(&(pointer as usize))
                .is_some()
        );
    }
    if current().pop_after_unpin.load(Ordering::Acquire) {
        current().pop_error.store(201, Ordering::Release);
    }
    status(error)
}
unsafe extern "C" fn pci_bus_id(
    out: *mut std::ffi::c_char,
    length: c_int,
    device: c_int,
) -> Status {
    current().pci_device.store(device, Ordering::Release);
    let id = format!("0000:{device:02x}:00.0\0");
    assert!(id.len() <= length as usize);
    unsafe {
        std::ptr::copy_nonoverlapping(id.as_ptr(), out.cast(), id.len());
    }
    SUCCESS
}
