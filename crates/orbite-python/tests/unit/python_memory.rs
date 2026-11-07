use super::*;
use pyo3::{
    buffer::PyBuffer,
    prelude::*,
    types::{PyByteArray, PyMemoryView},
};

#[test]
fn each_export_lease_is_charged_even_for_duplicate_registration_aliases() {
    let e = TransferEngine::new(orbite::EngineConfig {
        max_registered_regions: 2,
        ..Default::default()
    })
    .unwrap();
    let mut storage = Box::new([7u8; 16]);
    let pointer = storage.as_mut_ptr().cast();
    let owner = Owner {
        keep: Arc::new(storage),
        kind: ExportKind::Host,
    };
    let witness = Arc::downgrade(&owner.keep);
    let mut memory = Memory::default();
    for _ in 0..2 {
        memory
            .register_owned(
                &e,
                &[pointer],
                &[16],
                Opt::default(),
                &[Some(owner.clone())],
            )
            .unwrap();
    }
    assert_eq!(
        memory.register_owned(
            &e,
            &[pointer],
            &[16],
            Opt::default(),
            &[Some(owner.clone())],
        ),
        Err(FULL)
    );
    assert_eq!(memory.regs[&(pointer as usize, 16)].owners.len(), 2);
    drop(owner);
    memory.unregister(&e, &[pointer], Some(&[16])).unwrap();
    assert!(witness.upgrade().is_some());
    memory.unregister(&e, &[pointer], Some(&[16])).unwrap();
    assert!(witness.upgrade().is_none());
}

#[test]
fn uncertain_cleanup_keeps_the_real_python_exporter_and_closed_slot() {
    Python::initialize();
    Python::attach(|py| {
        let bytes = PyByteArray::new(py, &[7; 16]);
        let view = PyMemoryView::from(bytes.as_any()).unwrap();
        let exported = PyBuffer::<u8>::get(&view).unwrap();
        let address = exported.buf_ptr() as usize;
        let owner: Arc<dyn Send + Sync> = Arc::new(view.unbind());
        drop(exported);
        let witness = Arc::downgrade(&owner);
        py.detach(|| {
            let h = crate::engine::create_using(|| Ok(orbite::EngineConfig::default())).unwrap();
            crate::engine::with(h, |e| {
                let mut memory = lock(&e.memory)?;
                memory.register_owned(
                    &e.core,
                    &[address as *mut c_void],
                    &[16],
                    Opt::default(),
                    &[Some(Owner {
                        keep: owner,
                        kind: ExportKind::Host,
                    })],
                )?;
                // Fault injection at the native-cleanup result boundary. Core
                // CUDA/verbs shims separately drive the actual failed destroy.
                memory.regs.get_mut(&(address, 16)).unwrap().error = Some(UNKNOWN);
                Ok(())
            })
            .unwrap();
            assert_eq!(crate::engine::destroy(h), Err(UNKNOWN));
            assert!(witness.upgrade().is_some());
            assert_eq!(crate::engine::destroy(h), Err(INVALID));
            assert!(witness.upgrade().is_some());
        });
        // The retained view still prevents resizing the actual Python allocation.
        assert!(
            bytes
                .call_method1("extend", (b"x".as_slice(),))
                .unwrap_err()
                .is_instance_of::<pyo3::exceptions::PyBufferError>(py)
        );
    });
}
