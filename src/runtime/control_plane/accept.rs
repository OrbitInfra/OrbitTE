//! Readiness and error policy for the control listener's single accept thread.
use super::*;
#[cfg(windows)]
use windows_sys::Win32::Networking::WinSock::{WSAEBADF, WSAEFAULT, WSAENOTSOCK};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Action {
    Retry,
    Wait,
    Backoff,
    Stop,
}

fn action(error: &io::Error) -> Action {
    use io::ErrorKind::*;
    match (error.kind(), error.raw_os_error()) {
        (Interrupted | ConnectionAborted | ConnectionReset | ConnectionRefused | TimedOut, _) => {
            Action::Retry
        }
        (WouldBlock, _) => Action::Wait,
        (InvalidInput, _) => Action::Stop,
        #[cfg(unix)]
        (_, Some(libc::EBADF | libc::ENOTSOCK | libc::EFAULT)) => Action::Stop,
        #[cfg(windows)]
        (_, Some(WSAEBADF | WSAENOTSOCK | WSAEFAULT)) => Action::Stop,
        // Linux also reports pending errors of the new socket from accept.
        #[cfg(target_os = "linux")]
        (
            _,
            Some(
                libc::ENETDOWN
                | libc::EPROTO
                | libc::ENOPROTOOPT
                | libc::EHOSTDOWN
                | libc::ENONET
                | libc::EHOSTUNREACH
                | libc::EOPNOTSUPP
                | libc::ENETUNREACH,
            ),
        ) => Action::Retry,
        // Includes EMFILE, ENFILE, ENOBUFS and ENOMEM. An unrecognized error
        // on an otherwise live listener gets the same bounded retry.
        _ => Action::Backoff,
    }
}

fn backoff(warned: &mut bool, error: &io::Error) -> Instant {
    if !*warned {
        log::warn!("control acceptor will retry after {IO_POLL:?}: {error}");
        *warned = true;
    }
    Instant::now() + IO_POLL
}

pub(super) fn run(
    mut poll: mio::Poll,
    mut accept: impl FnMut() -> io::Result<(TcpStream, Option<SocketAddr>)>,
    sender: mpsc::SyncSender<(TcpStream, Instant, Option<SocketAddr>)>,
    stopping: &AtomicBool,
    timeout: Duration,
) -> io::Result<()> {
    let mut events = mio::Events::with_capacity(3);
    let mut warned = false;
    let mut retry_at: Option<Instant> = None;
    while !stopping.load(Ordering::Acquire) {
        // Readability can wake the poll immediately under descriptor pressure.
        // Park the remainder of that interval. Shutdown wakes both waits.
        if let Some(at) = retry_at.take() {
            if let Some(left) = at.checked_duration_since(Instant::now()) {
                retry_at = Some(at);
                thread::park_timeout(left);
                continue;
            }
        }
        match accept() {
            Ok((stream, profile)) => {
                warned = false;
                if stream.set_nonblocking(false).is_ok() && stream.set_nodelay(true).is_ok() {
                    if let Ok(deadline) = deadline(timeout) {
                        // The fixed queue and worker count bound sockets and payloads.
                        let _ = sender.try_send((stream, deadline, profile));
                    }
                }
                continue;
            }
            Err(error) => match action(&error) {
                Action::Retry => continue,
                Action::Wait => {}
                Action::Backoff => retry_at = Some(backoff(&mut warned, &error)),
                Action::Stop => return Err(error),
            },
        }
        match poll.poll(&mut events, Some(IO_POLL)) {
            Ok(()) if retry_at.is_none() => warned = false,
            Ok(()) => {}
            Err(error) if action(&error) == Action::Stop => return Err(error),
            Err(error) if error.kind() != io::ErrorKind::Interrupted => {
                retry_at = Some(backoff(&mut warned, &error));
            }
            Err(_) => {}
        }
    }
    Ok(())
}

#[cfg(test)]
#[path = "../../../tests/unit/runtime/control_plane/accept.rs"]
pub(super) mod tests;
