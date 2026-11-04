use super::*;
use driver::Status;
use std::cell::Cell;

thread_local! { static FAULT: Cell<&'static str> = const { Cell::new("") }; }
unsafe extern "C" fn get(out: *mut driver::Context) -> Status {
    if FAULT.get() == "save" {
        return Status::CUDA_ERROR_INVALID_CONTEXT;
    }
    // SAFETY: the guard supplied a writable handle.
    unsafe { (driver().current)(out) }
}
unsafe extern "C" fn push(ctx: driver::Context) -> Status {
    // SAFETY: forward the retained owner context.
    let code = unsafe { (driver().push)(ctx) };
    if FAULT.get() == "switch" {
        Status::CUDA_ERROR_INVALID_CONTEXT
    } else {
        code
    }
}
unsafe extern "C" fn pop(ctx: *mut driver::Context) -> Status {
    // SAFETY: forward the guard's output handle before injecting a restore error.
    let copied = current().events.lock().unwrap().contains(&"DtoH");
    // SAFETY: ctx is the writable output of the actual guard.
    let code = unsafe { (driver().pop)(ctx) };
    if copied && matches!(FAULT.get(), "pop" | "restore") {
        STACK.with(|s| {
            s.borrow_mut().pop();
        });
        Status::CUDA_ERROR_INVALID_CONTEXT
    } else {
        code
    }
}
unsafe extern "C" fn set(ctx: driver::Context) -> Status {
    if FAULT.get() == "restore" {
        return Status::CUDA_ERROR_ILLEGAL_ADDRESS;
    }
    // SAFETY: ctx is the exact saved caller binding, including null.
    unsafe { (driver().set_current)(ctx) }
}

#[test]
fn r1c_platform_copy_preserves_exact_custom_or_absent_context_and_stops_on_errors() -> Result<()> {
    for fault in ["", "copy", "save", "switch", "pop", "restore", "unpin"] {
        for bound in [false, true] {
            let owner = allocation(&[0x53; 32]);
            let caller = allocation(&[0x93; 32]); // same ordinal, different CUDA context
            let mut d = driver();
            let table = Arc::get_mut(&mut d).unwrap();
            table.current = get;
            table.set_current = set;
            table.push = push;
            table.pop = pop;
            // SAFETY: the fixture pins its byte allocation through registration and drain.
            let backing = unsafe { &*owner.bytes.get() };
            let address = backing.as_ptr() as u64;
            let m = Arc::new(CudaMemory::register(address, 32, owner.clone(), d)?);
            let engine = TransferEngine::new(Default::default())?;
            let view = engine
                .register_cuda_memory_batch(&[(m, 0, 32)], Permission::LocalOnly)?
                .remove(0);
            if bound {
                STACK.with(|s| s.borrow_mut().push(Arc::as_ptr(&caller) as usize));
            }
            let expected = STACK.with(|s| s.borrow().clone());
            owner.events.lock().unwrap().clear();
            if fault == "copy" {
                owner.copy_error.store(700, Ordering::Release);
            }
            if fault == "unpin" {
                owner.pop_after_unpin.store(true, Ordering::Release);
            }
            FAULT.set(fault);
            let mut bytes = [0xcc; 8];
            let result = view.read(2, &mut bytes[1..7]);
            // A handled device error never reaches the CPU memcpy branch, even though
            // the backing has a real, host-readable shim address.
            let actual = STACK.with(|s| std::mem::take(&mut *s.borrow_mut()));
            FAULT.set("");
            if fault != "restore" {
                assert_eq!(actual, expected, "{fault}");
            }
            match fault {
                "" => {
                    result?;
                    assert_eq!(bytes, [0xcc, 0x53, 0x53, 0x53, 0x53, 0x53, 0x53, 0xcc]);
                }
                "pop" | "restore" | "unpin" => {
                    assert!(matches!(result, Err(Error::Unquiesced { .. })))
                }
                _ => {
                    assert!(matches!(result, Err(Error::Backend { .. })));
                    assert_eq!(bytes, [0xcc; 8]);
                }
            }
            let events = owner.events.lock().unwrap();
            if matches!(fault, "save" | "switch") {
                assert!(!events.contains(&"DtoH"));
            }
            if fault == "save" {
                assert!(!events.contains(&"push"));
            }
            drop(events);
            // Host-only dispatch never consults a driver or changes a binding.
            let host = engine.register_memory(vec![0x46; 4].into(), Permission::LocalOnly)?;
            FAULT.set("save");
            host.read(0, &mut bytes[..4])?;
            FAULT.set("");
            assert_eq!(bytes[..4], [0x46; 4]);
        }
    }
    Ok(())
}
