#![allow(unsafe_code)]

use std::cell::{RefCell, UnsafeCell};
use std::collections::BTreeMap;
use std::ffi::{c_int, c_uint, c_void};
use std::sync::atomic::AtomicI32;
use std::sync::{OnceLock, Weak};
use std::time::Duration;

use super::*;
use crate::{
    BatchState, EngineConfig, Permission, TransferEngine, TransferOp, TransferRequest, Transport,
};
use driver::Status;

// The shim owns actual host backing and records Driver API boundaries. These
// tests prove CPU ownership contracts, not GPU execution or memory ordering.
struct Device {
    bytes: UnsafeCell<Box<[u8]>>,
    pins: Mutex<BTreeMap<usize, Box<[u8]>>>,
    copy_error: AtomicI32,
    sync_error: AtomicI32,
    pop_error: AtomicI32,
    free_error: AtomicI32,
    memory_type: AtomicI32,
    managed: AtomicI32,
    events: Mutex<Vec<&'static str>>,
}
// SAFETY: device bytes are accessed exclusively via one CudaMemory lock; pinned
// buffers are private to each copy and the map serializes allocation/deallocation.
unsafe impl Send for Device {}
unsafe impl Sync for Device {}
thread_local! { static STACK: RefCell<Vec<usize>> = const { RefCell::new(Vec::new()) }; }
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
unsafe extern "C" fn to_host(output: *mut c_void, address: u64, length: usize) -> Status {
    event("DtoH");
    let error = current().copy_error.load(Ordering::Acquire);
    if error == 0 {
        unsafe {
            std::ptr::copy_nonoverlapping(address as *const u8, output.cast(), length);
        }
    }
    status(error)
}
unsafe extern "C" fn to_device(address: u64, input: *const c_void, length: usize) -> Status {
    event("HtoD");
    let error = current().copy_error.load(Ordering::Acquire);
    // A synchronized backend error may still have changed a prefix of GPU
    // bytes. Logical failure is not a rollback receipt.
    let copied = if error == 0 { length } else { length.min(1) };
    unsafe {
        std::ptr::copy_nonoverlapping(input.cast::<u8>(), address as *mut u8, copied);
    }

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
fn driver() -> Arc<Driver> {
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
        host_allocate,
        host_free,
        pointer_attribute: attribute,
        address_range: range,
    })
}
fn allocation(bytes: &[u8]) -> Arc<Device> {
    let device = Arc::new(Device {
        bytes: UnsafeCell::new(bytes.to_vec().into_boxed_slice()),
        pins: Mutex::default(),
        copy_error: AtomicI32::new(0),
        sync_error: AtomicI32::new(0),
        pop_error: AtomicI32::new(0),
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
fn memory(device: &Arc<Device>) -> Arc<CudaMemory> {
    let bytes = unsafe { &*device.bytes.get() };
    Arc::new(
        CudaMemory::register(bytes.as_ptr() as u64, bytes.len(), device.clone(), driver()).unwrap(),
    )
}

#[test]
fn cuda_registration_checks_type_extent_and_restores_context_on_copy_errors() {
    let owner = allocation(&[0x5a; 16]);
    let address = unsafe { &*owner.bytes.get() }.as_ptr() as u64;
    owner.memory_type.store(1, Ordering::Release);
    assert!(matches!(
        CudaMemory::register(address, 16, owner.clone(), driver()),
        Err(Error::Unavailable(_))
    ));
    owner.memory_type.store(2, Ordering::Release);
    assert!(matches!(
        CudaMemory::register(address + 8, 9, owner.clone(), driver()),
        Err(Error::InvalidRange)
    ));
    owner.managed.store(1, Ordering::Release);
    assert!(matches!(
        CudaMemory::register(address, 16, owner.clone(), driver()),
        Err(Error::Unavailable(_))
    ));
    owner.managed.store(0, Ordering::Release);
    let memory = memory(&owner);
    memory.write(4, b"orbit").unwrap();
    let mut bytes = [0; 16];
    memory.read(0, &mut bytes).unwrap();
    assert_eq!(&bytes[..4], &[0x5a; 4]);
    assert_eq!(&bytes[4..9], b"orbit");
    assert_eq!(&bytes[9..], &[0x5a; 7]);
    owner.copy_error.store(1, Ordering::Release);
    let mut untouched = [0xcc; 8];
    assert!(matches!(
        memory.read(4, &mut untouched),
        Err(Error::Backend { code: 1, .. })
    ));
    assert_eq!(untouched, [0xcc; 8]);
    assert!(owner.pins.lock().unwrap().is_empty());
    assert!(STACK.with(|s| s.borrow().is_empty()));
    assert!(matches!(
        memory.write(1, &[0x19; 4]),
        Err(Error::Backend { code: 1, .. })
    ));
    owner.copy_error.store(0, Ordering::Release);
    let mut changed = [0; 16];
    memory.read(0, &mut changed).unwrap();
    assert_eq!(changed[1], 0x19);
    assert_eq!(changed[2], 0x5a);
    let events = owner.events.lock().unwrap();
    let error_copy = events
        .iter()
        .enumerate()
        .filter(|(_, e)| **e == "DtoH")
        .nth(1)
        .unwrap()
        .0;
    assert_eq!(
        &events[error_copy..error_copy + 3],
        &["DtoH", "sync", "pop"]
    );
}

#[test]
fn cuda_registered_ranges_use_actual_owners_and_tcp_read_write_paths() {
    let owner = allocation(&[0x7b; 32]);
    let weak = Arc::downgrade(&owner);
    let allocation = memory(&owner);
    let source = TransferEngine::new(EngineConfig::default()).unwrap();
    let source_memory = source
        .register_cuda_memory_batch(
            &[(allocation.clone(), 0, 32), (allocation.clone(), 0, 32)],
            Permission::RemoteReadWrite,
        )
        .unwrap();
    assert_eq!(source_memory[0].id(), source_memory[1].id());
    let requester = TransferEngine::new(EngineConfig::default()).unwrap();
    let local = requester
        .register_memory(vec![0xa5; 32].into_boxed_slice(), Permission::LocalOnly)
        .unwrap();
    let peer = requester
        .open_segment(
            source.endpoint(),
            source.source_epoch(),
            Duration::from_secs(2),
        )
        .unwrap();
    let request = TransferRequest {
        transport: Transport::Tcp,
        local_address: local.base_address() + 5,
        remote_address: source_memory[0].base_address() + 7,
        length: 13,
    };
    let report = requester
        .submit_and_wait(
            TransferOp::Read,
            &peer,
            std::slice::from_ref(&request),
            Duration::from_secs(2),
        )
        .unwrap();
    assert!(report.is_complete() && report.locally_drained);
    let mut bytes = [0; 32];
    local.read(0, &mut bytes).unwrap();
    assert_eq!(&bytes[5..18], &[0x7b; 13]);
    assert_eq!(&bytes[..5], &[0xa5; 5]);
    assert_eq!(&bytes[18..], &[0xa5; 14]);
    local.write(5, &[0x39; 13]).unwrap();
    assert!(
        requester
            .submit_and_wait(TransferOp::Write, &peer, &[request], Duration::from_secs(2))
            .unwrap()
            .is_complete()
    );
    source_memory[0].read(0, &mut bytes).unwrap();
    assert_eq!(&bytes[7..20], &[0x39; 13]);
    assert_eq!(&bytes[..7], &[0x7b; 7]);
    assert_eq!(&bytes[20..], &[0x7b; 12]);
    for region in &source_memory {
        source.unregister_memory(region).unwrap();
    }
    drop(owner);
    drop(allocation);
    assert!(weak.upgrade().is_some());
    drop(source_memory);
    // A wire ACK precedes the source handler dropping its physical owners.
    source.close_source(Duration::from_secs(2)).unwrap();
    assert!(weak.upgrade().is_none());
}

#[test]
fn unproven_cuda_copy_keeps_batch_backing_capacity_and_requester_admission() {
    let source = TransferEngine::new(EngineConfig::default()).unwrap();
    let remote = source
        .register_memory(vec![7; 8].into_boxed_slice(), Permission::RemoteRead)
        .unwrap();
    let requester = Arc::new(
        TransferEngine::new(EngineConfig {
            max_allocated_batches: 1,
            max_inflight_batches: 1,
            ..EngineConfig::default()
        })
        .unwrap(),
    );
    let owner = allocation(&[0xcc; 8]);
    let weak = Arc::downgrade(&owner);
    let allocation = memory(&owner);
    let memory = requester
        .register_cuda_memory_batch(&[(allocation.clone(), 0, 8)], Permission::LocalOnly)
        .unwrap()
        .remove(0);
    let peer = requester
        .open_segment(
            source.endpoint(),
            source.source_epoch(),
            Duration::from_secs(2),
        )
        .unwrap();
    owner.sync_error.store(700, Ordering::Release);
    let mut batch = requester
        .submit_async(
            TransferOp::Read,
            &peer,
            &[TransferRequest {
                transport: Transport::Tcp,
                local_address: memory.base_address(),
                remote_address: remote.base_address(),
                length: 8,
            }],
            None,
            Duration::from_secs(2),
        )
        .unwrap();
    let status = batch.wait(Duration::from_secs(2)).unwrap();
    assert_eq!(status.state, BatchState::Failed);
    assert!(!status.locally_drained);
    assert!(!batch.task_status(0).unwrap().locally_drained);
    assert!(!batch.submissions().unwrap()[0].locally_drained);
    assert!(matches!(batch.free(), Err(Error::BatchBusy)));
    requester.unregister_memory(&memory).unwrap();
    drop(memory);
    drop(allocation);
    drop(owner);
    assert!(weak.upgrade().is_some());
    drop(batch);
    assert!(matches!(
        requester.allocate_batch(1),
        Err(Error::ResourceExhausted)
    ));
    assert!(matches!(
        requester.close_requester(Duration::from_millis(5)),
        Err(Error::Timeout)
    ));
    let stats = requester.stats().unwrap();
    assert_eq!(
        (
            stats.allocated_batches,
            stats.inflight_batches,
            stats.retained_regions
        ),
        (1, 1, 1)
    );
}

#[test]
fn source_cuda_sync_failure_retains_source_owner_and_prevents_false_drain() {
    let source = TransferEngine::new(EngineConfig::default()).unwrap();
    let owner = allocation(&[0x77; 8]);
    let weak = Arc::downgrade(&owner);
    let allocation = memory(&owner);
    let memory = source
        .register_cuda_memory_batch(&[(allocation.clone(), 0, 8)], Permission::RemoteRead)
        .unwrap()
        .remove(0);
    let requester = TransferEngine::new(EngineConfig::default()).unwrap();
    let local = requester
        .register_memory(vec![0xcc; 8].into_boxed_slice(), Permission::LocalOnly)
        .unwrap();
    let peer = requester
        .open_segment(
            source.endpoint(),
            source.source_epoch(),
            Duration::from_secs(2),
        )
        .unwrap();
    owner.sync_error.store(700, Ordering::Release);
    let report = requester
        .submit_and_wait(
            TransferOp::Read,
            &peer,
            &[TransferRequest {
                transport: Transport::Tcp,
                local_address: local.base_address(),
                remote_address: memory.base_address(),
                length: 8,
            }],
            Duration::from_secs(2),
        )
        .unwrap();
    assert!(!report.is_complete());
    assert!(report.locally_drained);
    let mut bytes = [0; 8];
    local.read(0, &mut bytes).unwrap();
    assert_eq!(bytes, [0xcc; 8]);
    source.unregister_memory(&memory).unwrap();
    drop(memory);
    drop(allocation);
    drop(owner);
    assert!(matches!(
        source.close_source(Duration::from_millis(5)),
        Err(Error::Timeout)
    ));
    assert_eq!(source.stats().unwrap().active_source_operations, 1);
    assert!(weak.upgrade().is_some());
}

#[test]
fn cuda_sync_restore_and_unpin_failures_retain_native_backing_and_poison_reuse() {
    for mode in ["sync", "restore", "unpin"] {
        std::thread::spawn(move || {
            let owner = allocation(&[0x7b; 8]);
            let weak = Arc::downgrade(&owner);
            let memory = memory(&owner);
            match mode {
                "sync" => owner.sync_error.store(700, Ordering::Release),
                "restore" => owner.pop_error.store(201, Ordering::Release),
                "unpin" => owner.free_error.store(1, Ordering::Release),
                _ => unreachable!(),
            }
            let mut bytes = [0xcc; 8];
            let result = memory.read(0, &mut bytes);
            if mode == "unpin" {
                assert!(result.is_ok());
                assert_eq!(bytes, [0x7b; 8]);
            } else {
                assert!(matches!(result, Err(Error::Unquiesced { .. })));
                assert_eq!(bytes, [0xcc; 8]);
            }
            assert!(!owner.pins.lock().unwrap().is_empty());
            assert!(matches!(
                memory.read(0, &mut bytes),
                Err(Error::Unquiesced { .. })
            ));
            drop(memory);
            drop(owner);
            assert!(weak.upgrade().is_some());
        })
        .join()
        .unwrap();
    }
}
