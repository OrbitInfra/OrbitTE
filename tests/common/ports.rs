use std::net::TcpListener;

// Reserve an explicit test port outside Linux's automatic allocation range.
// A TCP connection alone does not stop another SO_REUSEADDR listener binding
// port zero from taking the port between two explicit binds. The UDP socket
// excludes other fixture processes during that handoff.
#[cfg(target_os = "linux")]
pub(crate) fn guarded_port() -> (TcpListener, std::net::UdpSocket) {
    use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    let sysctl = std::fs::read_to_string("/proc/sys/net/ipv4/ip_local_port_range").unwrap();
    let range: Vec<u16> = sysctl
        .split_whitespace()
        .map(|s| s.parse().unwrap())
        .collect();
    let ephemeral = range[0]..=range[1];
    // Prefer the upper unprivileged range, and fall back to the lower one
    // on hosts whose ephemeral range reaches the top of the port space.
    let base = NEXT.fetch_add(1, Relaxed);
    for i in 0..(65536 - 1024) {
        let port = (1024 + (base.wrapping_add(i).wrapping_add(63000) % (65536 - 1024))) as u16;
        if ephemeral.contains(&port) {
            continue;
        }
        let addr = (std::net::Ipv4Addr::LOCALHOST, port);
        if let Ok(guard) = std::net::UdpSocket::bind(addr) {
            if let Ok(listener) = TcpListener::bind(addr) {
                return (listener, guard);
            }
        }
    }
    panic!("no free loopback fixture port outside {ephemeral:?}");
}

#[cfg(not(target_os = "linux"))]
pub(crate) fn guarded_port() -> (TcpListener, [std::net::TcpStream; 2]) {
    let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let client = std::net::TcpStream::connect_timeout(
        &listener.local_addr().unwrap(),
        crate::test_support::EVENTUAL_PROGRESS,
    )
    .unwrap();
    let (accepted, _) = listener.accept().unwrap();
    (listener, [client, accepted])
}
