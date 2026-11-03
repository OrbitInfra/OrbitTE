use super::*;
use crate::transport::gds::tests::{Fixture, Mock, WAIT, request, wait_enter};
use crate::{GdsMode, RemoteEffect, TaskState};

#[test]
fn gds_registers_the_cuda_view_once_consumes_readiness_and_uses_registered_offsets() -> Result<()> {
    let mock = Mock::new(GdsMode::NativeOnly);
    let f = Fixture::new(&mock, EngineConfig::default(), 32)?;
    let dev = allocation(b"0123456789abcdefghijklmnopqrstuv");
    let gpu = memory(&dev);
    let ranges = [(gpu.clone(), 4, 20)];
    let v = f
        .engine
        .register_cuda_memory_batch(&ranges, Permission::LocalOnly)?
        .remove(0);
    let alias = f
        .engine
        .register_cuda_memory_batch(&ranges, Permission::LocalOnly)?
        .remove(0);
    let producer = super::readiness::Producer::new(&gpu, 0x38, true);
    let weak = Arc::downgrade(&producer);
    producer.enqueue(gpu.base_address() + 6, 8);
    producer.record(&gpu)?;
    drop(producer);
    let r = [request(&v, 2, 3, 8)];
    let b = f
        .engine
        .submit_async(TransferOp::Write, &f.segment, &r, None, WAIT)?;
    assert_eq!(b.wait(WAIT)?.completed_tasks, 1);
    assert!(weak.upgrade().is_none());
    let mut expected = [0; 32];
    expected[3..11].fill(0x38);
    assert_eq!(std::fs::read(&f.path)?, expected);
    v.write(0, &[0xbd; 20])?;
    assert!(
        f.engine
            .submit_and_wait(TransferOp::Read, &f.segment, &r, WAIT)?
            .is_complete()
    );
    let mut actual = [0; 20];
    v.read(0, &mut actual)?;
    let mut expected = [0xbd; 20];
    expected[2..10].fill(0x38);
    assert_eq!(actual, expected);
    assert_eq!(mock.buffer_registers.load(Ordering::Acquire), 1);
    f.engine.unregister_memory(&v)?;
    assert_eq!(mock.buffer_deregisters.load(Ordering::Acquire), 0);
    f.engine.unregister_memory(&alias)?;
    assert_eq!(mock.buffer_deregisters.load(Ordering::Acquire), 1);
    Ok(())
}

#[test]
fn gds_buffer_registration_failure_rolls_back_without_publishing_memory() -> Result<()> {
    let mock = Mock::new(GdsMode::NativeOnly);
    mock.register_error.store(true, Ordering::Release);
    let f = Fixture::new(&mock, EngineConfig::default(), 16)?;
    let dev = allocation(&[0; 16]);
    let gpu = memory(&dev);
    assert!(matches!(
        f.engine
            .register_cuda_memory_batch(&[(gpu.clone(), 0, 16)], Permission::LocalOnly),
        Err(Error::Backend { .. })
    ));
    assert!(
        f.engine
            .resolve_local_address(gpu.base_address(), 1)
            .is_err()
    );
    assert_eq!(f.engine.stats()?.retained_regions, 0);
    assert_eq!(mock.buffer_deregisters.load(Ordering::Acquire), 0);
    Ok(())
}

#[test]
fn gds_final_unregister_waits_for_batch_and_failed_deregister_retains_real_owners() -> Result<()> {
    for fail in [false, true] {
        let mock = Mock::new(GdsMode::NativeOnly);
        let f = Fixture::new(&mock, EngineConfig::default(), 16)?;
        let dev = allocation(&[0x37; 16]);
        let gpu = memory(&dev);
        let v = f
            .engine
            .register_cuda_memory_batch(&[(gpu, 0, 16)], Permission::LocalOnly)?
            .remove(0);
        let host = f.host(2)?;
        mock.held.store(true, Ordering::Release);
        mock.deregister_error.store(fail, Ordering::Release);
        let b = f.engine.submit_async(
            TransferOp::Write,
            &f.segment,
            &[request(&host, 0, 12, 2), request(&v, 0, 0, 8)],
            None,
            WAIT,
        )?;
        wait_enter(&mock);
        f.engine.unregister_memory(&v)?;
        assert!(f.engine.resolve_local_address(v.base_address(), 1).is_err());
        assert_eq!(mock.buffer_deregisters.load(Ordering::Acquire), 0);
        assert!(v.write(0, &[1]).is_err());
        mock.held.store(false, Ordering::Release);
        let status = b.wait(WAIT)?;
        assert_eq!(status.locally_drained, !fail);
        assert_eq!(mock.buffer_deregisters.load(Ordering::Acquire), 1);
        assert_eq!(mock.destroys.load(Ordering::Acquire), 1);
        let first = b.task_status(0)?;
        assert_eq!(
            (first.state, first.transferred_bytes),
            (TaskState::Completed, 2)
        );
        let out = b.task_status(1)?;
        assert_eq!(out.transferred_bytes, 8);
        let reports = b.submissions()?;
        let report = &reports[0];
        if fail {
            assert_eq!(
                (report.completed_requests, report.transferred_bytes),
                (1, 2)
            );
            let span = report.failure.as_ref().unwrap();
            assert_eq!((span.request_index, span.affected_requests), (1, 1));
            assert_eq!(out.state, TaskState::Failed);
            let failure = out.failure.unwrap();
            assert!(matches!(failure.error, Error::Unquiesced { .. }));
            assert_eq!(failure.remote_effect, RemoteEffect::Unknown);
            assert!(v.write(0, &[1]).is_err());
            assert!(
                f.engine
                    .close_file_segment(&f.segment, Duration::ZERO)
                    .is_err()
            );
            assert!(host.write(0, &[1]).is_err());
            assert_eq!(f.engine.stats()?.inflight_bytes, 10);
            assert_eq!(f.engine.stats()?.gds.quarantined_batches, 1);
            assert_eq!(mock.buffer_deregisters.load(Ordering::Acquire), 1);
        } else {
            assert_eq!(
                (report.completed_requests, report.transferred_bytes),
                (2, 10)
            );
            assert_eq!(out.state, TaskState::Completed);
            v.write(0, &[1])?;
            f.engine.close_file_segment(&f.segment, WAIT)?;
        }
    }
    Ok(())
}

#[test]
fn gds_idle_deregister_error_keeps_the_backing_and_registration_budget() -> Result<()> {
    let mock = Mock::new(GdsMode::NativeOnly);
    let f = Fixture::new(&mock, EngineConfig::default(), 16)?;
    let dev = allocation(&[0; 16]);
    let owner = Arc::downgrade(&dev);
    let gpu = memory(&dev);
    let v = f
        .engine
        .register_cuda_memory_batch(&[(gpu, 0, 16)], Permission::LocalOnly)?
        .remove(0);
    mock.deregister_error.store(true, Ordering::Release);
    assert!(matches!(
        f.engine.unregister_memory(&v),
        Err(Error::Unquiesced { .. })
    ));
    drop(v);
    drop(dev);
    assert!(owner.upgrade().is_some());
    assert_eq!(f.engine.stats()?.retained_bytes, 16);
    assert_eq!(mock.buffer_deregisters.load(Ordering::Acquire), 1);
    Ok(())
}

#[test]
fn queued_cancel_with_failed_last_deregister_keeps_admission_without_submitting() -> Result<()> {
    let mock = Mock::new(GdsMode::NativeOnly);
    let f = Fixture::new(
        &mock,
        EngineConfig {
            runtime_queue: Some(crate::RuntimeQueueConfig {
                max_dispatch_owners: 1,
                ..Default::default()
            }),
            ..Default::default()
        },
        32,
    )?;
    let host = f
        .engine
        .register_memory(vec![0x31; 16].into_boxed_slice(), Permission::LocalOnly)?;
    mock.held.store(true, Ordering::Release);
    let running = f.engine.submit_async(
        TransferOp::Write,
        &f.segment,
        &[request(&host, 0, 0, 4)],
        None,
        WAIT,
    )?;
    wait_enter(&mock);
    let dev = allocation(&[0x79; 16]);
    let gpu = f
        .engine
        .register_cuda_memory_batch(&[(memory(&dev), 0, 16)], Permission::LocalOnly)?
        .remove(0);
    let queued = f.engine.submit_async(
        TransferOp::Write,
        &f.segment,
        &[request(&gpu, 0, 8, 8)],
        None,
        WAIT,
    )?;
    f.engine.unregister_memory(&gpu)?;
    assert_eq!(mock.buffer_deregisters.load(Ordering::Acquire), 0);
    mock.deregister_error.store(true, Ordering::Release);
    assert!(queued.cancel(0)?);
    assert!(!queued.wait(WAIT)?.locally_drained);
    let tasks = queued.task_statuses()?;
    let failure = tasks[0].failure.as_ref().unwrap();
    assert!(matches!(failure.error, Error::Unquiesced { .. }));
    assert_eq!(failure.remote_effect, RemoteEffect::None);
    assert_eq!(mock.submits.load(Ordering::Acquire), 1);
    assert_eq!(mock.buffer_deregisters.load(Ordering::Acquire), 1);
    assert_eq!(f.engine.stats()?.gds.quarantined_batches, 1);
    mock.held.store(false, Ordering::Release);
    assert_eq!(running.wait(WAIT)?.completed_tasks, 1);
    assert_eq!(f.engine.stats()?.inflight_bytes, 8);
    assert_eq!(&std::fs::read(&f.path)?[8..16], &[0; 8]);
    Ok(())
}
