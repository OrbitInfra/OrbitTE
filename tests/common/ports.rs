use crate::test_support::EVENTUAL_PROGRESS;
use std::net::{TcpListener, TcpStream};

// A live accepted connection keeps the local port out of automatic port
// allocation. Dropping the listener permits an explicit rebind at the same
// address. Keep the pair alive across that handoff and any final rebind.
pub(crate) fn guarded_port() -> (TcpListener, [TcpStream; 2]) {
    let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let client =
        TcpStream::connect_timeout(&listener.local_addr().unwrap(), EVENTUAL_PROGRESS).unwrap();
    let (accepted, _) = listener.accept().unwrap();
    (listener, [client, accepted])
}
