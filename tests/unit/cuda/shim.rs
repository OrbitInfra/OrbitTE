use std::cell::{RefCell, UnsafeCell};
use std::collections::BTreeMap;
use std::ffi::{c_int, c_uint, c_void};
use std::sync::atomic::AtomicI32;
use std::sync::{OnceLock, Weak};

use super::*;
use driver::Status;

// The shim owns actual host backing and records Driver API boundaries. These
// tests prove CPU ownership contracts, not GPU execution or memory ordering.
pub(super) struct Device {
    pub(super) bytes: UnsafeCell<Box<[u8]>>,
    pub(super) pins: Mutex<BTreeMap<usize, Box<[u8]>>>,
    pub(super) streams: Mutex<BTreeMap<usize, Option<PendingCopy>>>,
    pub(super) completions: Mutex<BTreeMap<usize, Option<usize>>>,
    next_handle: AtomicUsize,
    pub(super) stream_create_error: AtomicI32,
    pub(super) event_create_error: AtomicI32,
    pub(super) record_error: AtomicI32,
    pub(super) stream_destroy_error: AtomicI32,
    pub(super) event_destroy_error: AtomicI32,
    pub(super) copy_error: AtomicI32,
    pub(super) sync_error: AtomicI32,
    pub(super) pop_error: AtomicI32,
    pub(super) pop_after_completion: AtomicBool,
    pub(super) free_error: AtomicI32,
    pub(super) memory_type: AtomicI32,
    pub(super) managed: AtomicI32,
    pub(super) events: Mutex<Vec<&'static str>>,
}
pub(super) struct PendingCopy {
    source: usize,
    destination: usize,
    length: usize,
}

// SAFETY: device bytes are accessed exclusively via one CudaMemory lock; pinned
// buffers are private to each copy and the map serializes allocation/deallocation.
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
    unreachable!()
}
unsafe extern "C" fn memset(_: u64, _: u8, _: usize) -> Status {
    unreachable!()
}
unsafe extern "C" fn stream_create(output: *mut sys::CUstream, flags: c_uint) -> Status {
    event("stream-create");
    assert_eq!(flags, sys::CUstream_flags::CU_STREAM_NON_BLOCKING as u32);
    let error = current().stream_create_error.load(Ordering::Acquire);
    if error == 0 {
        let handle = current().next_handle.fetch_add(1, Ordering::Relaxed);
        current().streams.lock().unwrap().insert(handle, None);
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
        assert!(pending.is_none(), "destroy is not a DMA drain receipt");
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
        if let Some(copy) = current()
            .streams
            .lock()
            .unwrap()
            .get_mut(&stream)
            .unwrap()
            .take()
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
unsafe extern "C" fn event_synchronize(completion: sys::CUevent) -> Status {
    event("event-sync");
    let stream = current().completions.lock().unwrap()[&(completion as usize)]
        .expect("unrecorded event cannot prove this copy complete");
    let result = drain(stream);
    if current().pop_after_completion.load(Ordering::Acquire) {
        current().pop_error.store(201, Ordering::Release);
    }
    result
}
fn enqueue(stream: sys::CUstream, source: usize, destination: usize, length: usize) {
    let previous = current()
        .streams
        .lock()
        .unwrap()
        .get_mut(&(stream as usize))
        .unwrap()
        .replace(PendingCopy {
            source,
            destination,
            length,
        });
    assert!(previous.is_none(), "copy/event reused with pending DMA");
}
unsafe extern "C" fn to_host(
    output: *mut c_void,
    address: u64,
    length: usize,
    stream: sys::CUstream,
) -> Status {
    event("DtoH");
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
    SUCCESS
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
            PointerAttribute::CU_POINTER_ATTRIBUTE_DEVICE_ORDINAL => *output.cast::<i32>() = 0,
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
        primary_retain: retain,
        primary_release: release,
        push,
        pop,
        synchronize: sync,
        allocate,
        free,
        memset,
        to_host,
        to_device,
        stream_create,
        stream_destroy,
        stream_synchronize,
        event_create,
        event_destroy,
        event_record,
        event_synchronize,
        host_allocate,
        host_free,
        pointer_attribute: attribute,
        address_range: range,
    })
}
pub(super) fn allocation(bytes: &[u8]) -> Arc<Device> {
    let device = Arc::new(Device {
        bytes: UnsafeCell::new(bytes.to_vec().into_boxed_slice()),
        pins: Mutex::default(),
        streams: Mutex::default(),
        completions: Mutex::default(),
        next_handle: AtomicUsize::new(1),
        stream_create_error: AtomicI32::new(0),
        event_create_error: AtomicI32::new(0),
        record_error: AtomicI32::new(0),
        stream_destroy_error: AtomicI32::new(0),
        event_destroy_error: AtomicI32::new(0),
        copy_error: AtomicI32::new(0),
        sync_error: AtomicI32::new(0),
        pop_error: AtomicI32::new(0),
        pop_after_completion: AtomicBool::new(false),
        free_error: AtomicI32::new(0),
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
