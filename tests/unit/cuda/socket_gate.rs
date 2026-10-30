use std::io::{Read, Write, copy};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::sync::mpsc;
use std::time::Duration;

// Hold bytes on a real control connection without stopping the source owner.
// Releasing this gate, rather than requester terminal, permits source EOF cleanup.
pub(super) struct SocketGate {
    pub(super) endpoint: SocketAddr,
    pub(super) reached: mpsc::Receiver<()>,
    pub(super) closed: mpsc::Receiver<()>,
    release: Option<mpsc::Sender<bool>>,
    relay: Option<std::thread::JoinHandle<()>>,
}

pub(super) enum Boundary {
    Pin(usize),
    #[cfg(feature = "rdma")]
    Native([u8; 36], usize),
    #[cfg(feature = "rdma")]
    Finish([u8; 36], usize, bool),
    #[cfg(feature = "rdma")]
    Setup {
        reply: [u8; 36],
        occurrence: usize,
        payload: usize,
        prefix: usize,
        corrupt: Option<usize>,
    },
}

impl SocketGate {
    pub(super) fn new(target: SocketAddr, boundary: Boundary) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = listener.local_addr().unwrap();
        let (entered, reached) = mpsc::channel();
        let (gone, closed) = mpsc::channel();
        let (release, resumed) = mpsc::channel();
        let relay = std::thread::spawn(move || {
            let (mut client, _) = listener.accept().unwrap();
            let mut server = TcpStream::connect(target).unwrap();
            client.set_nodelay(true).unwrap();
            server.set_nodelay(true).unwrap();
            server
                .set_read_timeout(Some(Duration::from_secs(10)))
                .unwrap();
            let mut input = client.try_clone().unwrap();
            let mut output = server.try_clone().unwrap();
            let upstream = std::thread::spawn(move || {
                let _ = copy(&mut input, &mut output);
                let _ = gone.send(());
            });
            #[cfg(feature = "rdma")]
            let mut rest = Vec::new();
            #[cfg(not(feature = "rdma"))]
            let rest = Vec::new();
            match boundary {
                Boundary::Pin(prefix) => {
                    let mut response = [0; 54];
                    server.read_exact(&mut response).unwrap();
                    client.write_all(&response[..prefix]).unwrap();
                }
                #[cfg(feature = "rdma")]
                native => {
                    let (needle, occurrence, frame) = match native {
                        Boundary::Native(needle, occurrence) => (needle, occurrence, None),
                        Boundary::Finish(needle, prefix, corrupt) => (
                            needle,
                            1,
                            Some((2 * 142, prefix, corrupt.then_some(36 + 142 - 1))),
                        ),
                        Boundary::Setup {
                            reply,
                            occurrence,
                            payload,
                            prefix,
                            corrupt,
                        } => (reply, occurrence, Some((payload, prefix, corrupt))),
                        Boundary::Pin(_) => unreachable!(),
                    };
                    // Match the complete Reply, including this source's epoch;
                    // carry every unrelated native/control byte unchanged.
                    let mut pending = Vec::new();
                    let mut count = 0;
                    loop {
                        let mut byte = [0];
                        server.read_exact(&mut byte).unwrap();
                        pending.push(byte[0]);
                        while !needle.starts_with(&pending) {
                            client.write_all(&pending[..1]).unwrap();
                            pending.remove(0);
                        }
                        if pending == needle {
                            count += 1;
                            if count == occurrence {
                                if let Some((payload, prefix, corrupt)) = frame {
                                    let mut frame = needle.to_vec();
                                    frame.resize(36 + payload, 0);
                                    server.read_exact(&mut frame[36..]).unwrap();
                                    if let Some(index) = corrupt {
                                        frame[index] ^= 1;
                                    }
                                    client.write_all(&frame[..prefix]).unwrap();
                                    rest.extend_from_slice(&frame[prefix..]);
                                } else {
                                    client.write_all(&needle[..13]).unwrap();
                                    rest.extend_from_slice(&needle[13..]);
                                }
                                break;
                            }
                            client.write_all(&pending).unwrap();
                            pending.clear();
                        }
                    }
                }
            }
            entered.send(()).unwrap();
            if resumed.recv() == Ok(true) && client.write_all(&rest).is_ok() {
                let _ = copy(&mut server, &mut client);
            }
            let _ = client.shutdown(Shutdown::Both);
            let _ = server.shutdown(Shutdown::Both);
            upstream.join().unwrap();
        });
        Self {
            endpoint,
            reached,
            closed,
            release: Some(release),
            relay: Some(relay),
        }
    }
}

#[cfg(feature = "rdma")]
impl SocketGate {
    pub(super) fn resume(&mut self) {
        self.release.take().unwrap().send(true).unwrap();
    }
}

impl Drop for SocketGate {
    fn drop(&mut self) {
        if let Some(release) = self.release.take() {
            let _ = release.send(false);
        }
        if let Some(relay) = self.relay.take() {
            let _ = relay.join();
        }
    }
}
