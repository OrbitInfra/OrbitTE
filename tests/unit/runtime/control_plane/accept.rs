use super::*;
use crate::{
    Permission, TransferEngine, TransferOp, TransferRequest, Transport,
    test_support::EVENTUAL_PROGRESS,
};
use std::cell::RefCell;

type BeforeAccept = Box<dyn FnMut() -> io::Result<()> + Send>;
thread_local! {
    // The next service constructed on this test thread takes the hook. Other
    // engines, including engines created by parallel tests, use real accept.
    static NEXT: RefCell<Option<BeforeAccept>> = const { RefCell::new(None) };
}

pub(in crate::runtime::control_plane) fn instrument(
    mut accept: impl FnMut() -> io::Result<TcpStream>,
) -> impl FnMut() -> io::Result<TcpStream> {
    let mut before = NEXT.with(|next| next.take());
    move || {
        if let Some(before) = &mut before {
            before()?;
        }
        accept()
    }
}

fn install(hook: impl FnMut() -> io::Result<()> + Send + 'static) {
    NEXT.with(|next| assert!(next.replace(Some(Box::new(hook))).is_none()));
}

fn read_with_guards(source: &TransferEngine) {
    let requester = TransferEngine::new(Default::default()).unwrap();
    let remote = source
        .register_memory(vec![0xda, 1, 2, 3, 0xdb].into(), Permission::RemoteRead)
        .unwrap();
    let local = requester
        .register_memory(vec![0xca; 5].into(), Permission::LocalOnly)
        .unwrap();
    let peer = requester
        .open_segment(source.endpoint(), source.source_epoch(), EVENTUAL_PROGRESS)
        .unwrap();
    let request = TransferRequest::new(
        Transport::Tcp,
        local.base_address() + 1,
        remote.base_address() + 1,
        3,
    );
    let report = requester
        .submit_and_wait(TransferOp::Read, &peer, &[request], EVENTUAL_PROGRESS)
        .unwrap();
    assert!(report.is_complete() && report.locally_drained, "{report:?}");
    let mut bytes = [0; 5];
    local.read(0, &mut bytes).unwrap();
    assert_eq!(bytes, [0xca, 1, 2, 3, 0xca]);
    remote.read(0, &mut bytes).unwrap();
    assert_eq!(bytes, [0xda, 1, 2, 3, 0xdb]);
}

#[test]
#[cfg(unix)]
#[allow(unsafe_code)]
fn reset_before_accept_preserves_the_listener_for_a_guarded_read() {
    use std::os::fd::AsRawFd;
    let (release, gate) = mpsc::sync_channel(1);
    let mut gate = Some(gate);
    install(move || {
        if let Some(gate) = gate.take() {
            gate.recv_timeout(EVENTUAL_PROGRESS).unwrap();
        }
        Ok(())
    });
    let source = TransferEngine::new(Default::default()).unwrap();
    let client = TcpStream::connect_timeout(&source.endpoint(), EVENTUAL_PROGRESS).unwrap();
    let linger = libc::linger {
        l_onoff: 1,
        l_linger: 0,
    };
    // SAFETY: the client owns its live fd; linger is a properly sized,
    // initialized input to setsockopt and remains valid for the call.
    let rc = unsafe {
        libc::setsockopt(
            client.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_LINGER,
            std::ptr::from_ref(&linger).cast(),
            std::mem::size_of_val(&linger) as libc::socklen_t,
        )
    };
    assert_eq!(rc, 0, "{}", io::Error::last_os_error());
    drop(client);
    release.send(()).unwrap();
    read_with_guards(&source);
}

#[test]
#[cfg(unix)]
fn exhausted_accept_waits_even_with_a_readable_backlog_then_serves_the_queued_socket() {
    let (release, gate) = mpsc::sync_channel(1);
    let mut gate = Some(gate);
    let (observed, attempts) = mpsc::sync_channel(5);
    let errors = [libc::EMFILE, libc::ENFILE, libc::ENOBUFS, libc::ENOMEM];
    let mut count = 0;
    install(move || {
        if let Some(gate) = gate.take() {
            gate.recv_timeout(EVENTUAL_PROGRESS).unwrap();
        }
        if count <= errors.len() {
            observed.send(Instant::now()).unwrap();
        }
        let error = errors.get(count).copied();
        count += 1;
        error.map_or(Ok(()), |code| Err(io::Error::from_raw_os_error(code)))
    });
    let source = TransferEngine::new(Default::default()).unwrap();
    let mem = source
        .register_memory(vec![0xa0, 7, 9, 0xaf].into(), Permission::RemoteRead)
        .unwrap();
    let mut queued =
        ControlChannel::connect(source.endpoint(), deadline(EVENTUAL_PROGRESS).unwrap()).unwrap();
    queued
        .write_all(
            &Request {
                operation: Operation::TcpRead,
                epoch: source.source_epoch(),
                region: mem.id(),
                offset: 1,
                length: 2,
            }
            .encode(),
        )
        .unwrap();
    release.send(()).unwrap();
    let reply = Reply::read(&mut queued, source.source_epoch()).unwrap();
    assert_eq!((reply.status, reply.length), (0, 2));
    let mut bytes = [0xc1; 4];
    queued.read_exact(&mut bytes[1..3]).unwrap();
    assert_eq!(bytes, [0xc1, 7, 9, 0xc1]);
    mem.read(0, &mut bytes).unwrap();
    assert_eq!(bytes, [0xa0, 7, 9, 0xaf]);
    let times: Vec<_> = (0..=errors.len())
        .map(|_| attempts.recv_timeout(EVENTUAL_PROGRESS).unwrap())
        .collect();
    assert!(
        times.windows(2).all(|pair| pair[1] - pair[0] >= IO_POLL),
        "retried resource pressure early: {times:?}"
    );
    read_with_guards(&source);
}

#[test]
fn peer_errors_are_retried_immediately_and_a_bad_listener_exits() {
    for kind in [
        io::ErrorKind::Interrupted,
        io::ErrorKind::ConnectionAborted,
        io::ErrorKind::ConnectionReset,
        io::ErrorKind::TimedOut,
    ] {
        assert_eq!(action(&kind.into()), Action::Retry);
    }
    let mut faults = [
        io::ErrorKind::ConnectionAborted,
        io::ErrorKind::ConnectionReset,
        io::ErrorKind::TimedOut,
    ]
    .into_iter();
    install(move || faults.next().map_or(Ok(()), |e| Err(e.into())));
    let source = TransferEngine::new(Default::default()).unwrap();
    read_with_guards(&source);
    let (sender, _rx) = mpsc::sync_channel(1);
    let poll = mio::Poll::new().unwrap();
    let err = run(
        poll,
        || Err(io::ErrorKind::InvalidInput.into()),
        sender,
        &AtomicBool::new(false),
        EVENTUAL_PROGRESS,
    )
    .unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    #[cfg(unix)]
    assert_eq!(
        action(&io::Error::from_raw_os_error(libc::EBADF)),
        Action::Stop
    );
}
