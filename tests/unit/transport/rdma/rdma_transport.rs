use super::*;
use crate::engine::deadline;
use crate::engine::tests::{rdma_fixture, source_runtime};
use crate::runtime::control_plane::{REQUEST_SIZE, u64_at};
use crate::transport::rdma::{
    DESCRIPTOR_SIZE,
    grant::RdmaGrant,
    path::NicPath,
    protocol::*,
    qp::{ConnectionId, Descriptor},
};
use crate::{Permission, RemoteRegion, TransferEngine, TransferRequest, Transport};
use std::{io, net::TcpListener, thread, time::Duration};

const TIMEOUT: Duration = Duration::from_secs(3);

fn identity(epoch: u8, domain: u8, generation: u64) -> Identity {
    let mut connection = [domain; 24];
    connection[16..].copy_from_slice(&generation.to_be_bytes());
    Identity {
        epoch: SourceEpoch([epoch; 16]),
        connection: ConnectionId::decode(&connection).unwrap(),
    }
}

fn channels() -> (ControlChannel<'static>, ControlChannel<'static>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let sender =
        ControlChannel::connect(listener.local_addr().unwrap(), deadline(TIMEOUT).unwrap())
            .unwrap();
    let receiver = ControlChannel {
        stream: listener.accept().unwrap().0,
        deadline: deadline(TIMEOUT).unwrap(),
        stopping: None,
    };
    (sender, receiver)
}

fn expect_closed_without_next_phase(channel: &mut ControlChannel<'_>) {
    let mut next = [0];
    let result = channel.read_exact(&mut next);
    assert!(
        matches!(&result, Err(Error::Io(error)) if matches!(error.kind(), io::ErrorKind::UnexpectedEof | io::ErrorKind::ConnectionReset)),
        "invalid peer metadata must close before another phase: {result:?}, byte {next:?}"
    );
}

fn request(epoch: SourceEpoch, region: u64) -> Request {
    Request {
        operation: Operation::RdmaWrite,
        epoch,
        region,
        offset: 0,
        length: 8,
    }
}

fn send_frame(channel: &mut ControlChannel<'_>, phase: u8, bytes: &[u8; BINDING_SIZE]) {
    channel.write_all(&[phase]).unwrap();
    channel.write_all(bytes).unwrap();
}

#[test]
fn identical_requests_on_identical_connections_cannot_replay_an_old_grant_receipt() {
    let source = identity(1, 2, 3);
    let requester = identity(4, 5, 6);
    let request = request(source.epoch, 7);
    let old = Binding::new(&request, source, requester, 1, 0).unwrap();
    let next = Binding::new(&request, source, requester, 2, 0).unwrap();
    for phase in [GRANT, DONE, ACK] {
        let (mut sender, mut receiver) = channels();
        old.write(&mut sender, phase).unwrap();
        assert!(matches!(
            next.read(&mut receiver, phase),
            Err(Error::Protocol(_))
        ));
    }
    assert!(Binding::new(&request, source, requester, 0, 0).is_err());
    let (mut sender, mut receiver) = channels();
    next.write(&mut sender, DONE).unwrap();
    assert!(matches!(
        next.read(&mut receiver, ACK),
        Err(Error::Protocol(_))
    ));
}

fn source_offer(
    source: &TransferEngine,
    request: &Request,
) -> (
    ControlChannel<'static>,
    Identity,
    u64,
    [u8; DESCRIPTOR_SIZE],
) {
    let mut channel =
        ControlChannel::connect(source.endpoint(), deadline(TIMEOUT).unwrap()).unwrap();
    channel.write_all(&request.encode()).unwrap();
    let (paths, _) = path::read_paths(&mut channel, source.source_epoch()).unwrap();
    Hello {
        requester: identity(9, 8, 7),
        source: None,
        expected_domain: paths[0].domain,
        direct: false,
        qp_width: 0,
        previous_grant: 0,
    }
    .write(&mut channel)
    .unwrap();
    let reply = Reply::read(&mut channel, source.source_epoch()).unwrap();
    assert_eq!((reply.status, reply.length), (0, OFFER_SIZE as u64));
    let mut offer = [0; OFFER_SIZE];
    channel.read_exact(&mut offer).unwrap();
    let identity = Identity::decode(&offer[..IDENTITY_SIZE]).unwrap();
    let grant = u64_at(&offer, IDENTITY_SIZE);
    let mut descriptor = [0; DESCRIPTOR_SIZE];
    descriptor.copy_from_slice(&offer[IDENTITY_SIZE + 8..OFFER_SIZE - 1]);
    Descriptor::decode_connection(&descriptor).unwrap();
    (channel, identity, grant, descriptor)
}

#[test]
fn source_rejects_stale_accept_before_connect_or_bind_and_releases_actual_owners() {
    // Complete control sockets and production owner paths; the verbs callbacks
    // are CPU controls and do not establish any physical RNIC qualification.
    for case in 0..12 {
        let source = rdma_fixture("direct:ok");
        let memory = source
            .register_memory(
                vec![0x42; 8].into_boxed_slice(),
                Permission::RemoteReadWrite,
            )
            .unwrap();
        let request = request(source.source_epoch(), memory.id());
        let (mut channel, local, grant, descriptor) = source_offer(&source, &request);
        let binding = Binding::new(&request, local, identity(9, 8, 7), grant, 0).unwrap();
        let mut bytes = binding.0;
        let mut phase = ACCEPT;
        match case {
            0 => phase = DONE,
            1..=8 => {
                let index = [10, 12, 35, 43, 51, 52, 68, 91][case - 1];
                bytes[index] ^= 1;
            }
            9 => bytes[BINDING_SIZE - 1] ^= 1,
            10 => {
                bytes[REQUEST_SIZE + IDENTITY_SIZE + 16..REQUEST_SIZE + IDENTITY_SIZE + 32].fill(0)
            }
            11 => bytes[REQUEST_SIZE + IDENTITY_SIZE + 32..BINDING_SIZE - 8].fill(0),
            _ => unreachable!(),
        }
        send_frame(&mut channel, phase, &bytes);
        // A correct-looking descriptor cannot repair the stale accept binding.
        let _ = channel.write_all(&descriptor);
        assert!(
            matches!(
                Reply::read(&mut channel, source.source_epoch()),
                Err(Error::Protocol(_))
            ),
            "case {case}"
        );
        let stats = source.stats().unwrap().rdma.unwrap();
        assert_eq!(
            (
                stats.completed_window_binds,
                stats.active_endpoints,
                stats.active_windows,
                stats.work_requests
            ),
            (0, 0, 0, 0),
            "case {case}"
        );
        let runtime_log = crate::transport::rdma::tests::events(source_runtime(&source));
        assert!(
            !runtime_log.lock().unwrap().contains(&"connect"),
            "case {case}"
        );
        assert_eq!((stats.window_key_indices, stats.burned_window_keys), (1, 1));
        let mut bytes = [0; 8];
        memory.read(0, &mut bytes).unwrap();
        assert_eq!(bytes, [0x42; 8]);
        source.close(TIMEOUT).unwrap();
        assert_eq!(source.stats().unwrap().rdma.unwrap().window_key_indices, 0);
    }
}

#[test]
fn source_done_requires_both_incarnations_request_grant_and_phase_before_staged_commit() {
    for index in [10, 35, 43, 51, 52, 68, 91, 92, 108, 131, 139, usize::MAX] {
        let source = rdma_fixture("ok");
        let memory = source
            .register_memory(
                vec![0x42; 8].into_boxed_slice(),
                Permission::RemoteReadWrite,
            )
            .unwrap();
        let request = request(source.source_epoch(), memory.id());
        let (mut channel, local, grant, descriptor) = source_offer(&source, &request);
        let binding = Binding::new(&request, local, identity(9, 8, 7), grant, 0).unwrap();
        binding.write(&mut channel, ACCEPT).unwrap();
        channel.write_all(&descriptor).unwrap();
        assert_eq!(
            Reply::read(&mut channel, source.source_epoch())
                .unwrap()
                .status,
            9
        );
        binding.read(&mut channel, GRANT).unwrap();
        channel.read_exact(&mut [0; DESCRIPTOR_SIZE]).unwrap();
        let mut bytes = binding.0;
        let phase = if index == usize::MAX {
            GRANT
        } else {
            bytes[index] ^= 1;
            DONE
        };
        send_frame(&mut channel, phase, &bytes);
        assert!(
            matches!(
                Reply::read(&mut channel, source.source_epoch()),
                Err(Error::Protocol(_))
            ),
            "field {index}"
        );
        let stats = source.stats().unwrap().rdma.unwrap();
        assert_eq!(
            (
                stats.completed_window_binds,
                stats.completed_window_invalidations,
                stats.active_endpoints,
                stats.active_windows,
                stats.work_requests
            ),
            (1, 0, 0, 0, 0)
        );
        let mut actual = [0; 8];
        memory.read(0, &mut actual).unwrap();
        assert_eq!(actual, [0x42; 8], "malformed DONE cannot commit staging");
        source.close(TIMEOUT).unwrap();
    }
}

fn grant_descriptor(address: u64) -> Descriptor {
    let mut bytes = [0; DESCRIPTOR_SIZE];
    bytes[..8].copy_from_slice(&address.to_be_bytes());
    bytes[8..12].copy_from_slice(&0x102u32.to_be_bytes());
    bytes[12..16].copy_from_slice(&42u32.to_be_bytes());
    bytes[16..20].copy_from_slice(&17u32.to_be_bytes());
    bytes[20..22].copy_from_slice(&1u16.to_be_bytes());
    bytes[22] = 3;
    bytes[23] = 1;
    Descriptor::decode(&bytes).unwrap()
}

#[test]
fn requester_checks_source_offer_and_each_bound_phase_before_data_or_read_copyback() {
    for (stage, field) in [(0, 0), (0, 1), (0, 2), (0, 3)].into_iter().chain(
        [10, 35, 43, 51, 52, 68, 91, 92, 108, 131, 139, usize::MAX]
            .into_iter()
            .flat_map(|field| [(1, field), (2, field)]),
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let epoch = SourceEpoch([7; 16]);
        let peer = RemoteSegment {
            endpoint: listener.local_addr().unwrap(),
            epoch,
            regions: vec![RemoteRegion {
                epoch,
                id: 1,
                address: 0x1000,
                length: 8,
                permission: Permission::RemoteReadWrite,
            }],
        };
        let handler = thread::spawn(move || {
            let mut channel = ControlChannel {
                stream: listener.accept().unwrap().0,
                deadline: deadline(TIMEOUT).unwrap(),
                stopping: None,
            };
            let wire = crate::runtime::control_plane::tests::read_request(&mut channel);
            path::write_paths(
                &mut channel,
                epoch,
                &[NicPath {
                    nic_id: 0,
                    name: "copy".into(),
                    domain: [8; 16],
                    gid_type: crate::transport::rdma::discovery::GidType::Ib,
                    descriptor: Descriptor {
                        lid: 1,
                        mtu: 3,
                        link_layer: 1,
                        ..Default::default()
                    },
                }],
                0,
            )
            .unwrap();
            let hello = Hello::read(&mut channel, epoch).unwrap();
            let payload = vec![0x42u8; 8].into_boxed_slice();
            let descriptor = grant_descriptor(payload.as_ptr() as u64);
            let local = identity(7, 8, 9);
            let grant = 1u64;
            let mut offer = [0; OFFER_SIZE];
            offer[..IDENTITY_SIZE].copy_from_slice(&local.encode());
            offer[IDENTITY_SIZE..IDENTITY_SIZE + 8].copy_from_slice(&grant.to_be_bytes());
            offer[IDENTITY_SIZE + 8..OFFER_SIZE - 1]
                .copy_from_slice(&descriptor.connection_bytes());
            if stage == 0 {
                match field {
                    0 => offer[0] ^= 1,
                    1 => offer[16..32].fill(0),
                    2 => offer[32..40].fill(0),
                    3 => offer[40..48].fill(0),
                    _ => unreachable!(),
                }
            }
            channel
                .write_all(
                    &Reply {
                        status: 0,
                        epoch,
                        length: OFFER_SIZE as u64,
                    }
                    .encode(),
                )
                .unwrap();
            channel.write_all(&offer).unwrap();
            if stage == 0 {
                expect_closed_without_next_phase(&mut channel);
                return;
            }
            let binding =
                Binding::accept(&mut channel, &wire, local, hello.requester, grant, 0).unwrap();
            channel.read_exact(&mut [0; DESCRIPTOR_SIZE]).unwrap();
            channel
                .write_all(
                    &Reply {
                        status: 9,
                        epoch,
                        length: 0,
                    }
                    .encode(),
                )
                .unwrap();
            let mut changed = binding.0;
            if field != usize::MAX {
                changed[field] ^= 1;
            }
            if stage == 1 {
                send_frame(
                    &mut channel,
                    if field == usize::MAX { ACK } else { GRANT },
                    &changed,
                );
                let _ = channel.write_all(&descriptor.encode());
                // Keep the backing live until the requester closes. A broken
                // GRANT check must fail on an unexpected DONE, not peer EOF.
                expect_closed_without_next_phase(&mut channel);
                return;
            }
            binding.write(&mut channel, GRANT).unwrap();
            channel.write_all(&descriptor.encode()).unwrap();
            binding.read(&mut channel, DONE).unwrap();
            channel
                .write_all(
                    &Reply {
                        status: 0,
                        epoch,
                        length: 0,
                    }
                    .encode(),
                )
                .unwrap();
            send_frame(
                &mut channel,
                if field == usize::MAX { DONE } else { ACK },
                &changed,
            );
            // Immutable payload remains alive until all requester data and its
            // actual QP close precede DONE; no physical DMA is simulated here.
            drop(payload);
        });
        let requester = rdma_fixture("copy");
        let local = requester
            .register_memory(vec![0xcc; 8].into_boxed_slice(), Permission::LocalOnly)
            .unwrap();
        let report = requester
            .submit_and_wait(
                TransferOp::Read,
                &peer,
                &[TransferRequest {
                    transport: Transport::Rdma,
                    local_address: local.base_address(),
                    remote_address: 0x1000,
                    length: 8,
                }],
                TIMEOUT,
            )
            .unwrap();
        handler.join().unwrap();
        assert_eq!(report.completed_requests, 0, "stage {stage} field {field}");
        let failure = report.failure.as_ref().unwrap();
        if stage == 0 && field == 0 {
            assert!(matches!(failure.error, Error::EpochMismatch));
        } else {
            assert!(matches!(failure.error, Error::Protocol(_)), "{failure:?}");
        }
        if stage == 0 {
            let log = crate::transport::rdma::tests::events(source_runtime(&requester));
            assert!(!log.lock().unwrap().contains(&"connect"));
        }
        assert!(report.locally_drained);
        let stats = requester.stats().unwrap().rdma.unwrap();
        assert_eq!(
            (stats.completed_reads, stats.posted_work_requests),
            if stage == 2 { (1, 2) } else { (0, 0) }
        );
        assert_eq!(
            (
                stats.active_endpoints,
                stats.staging_bytes,
                stats.work_requests
            ),
            (0, 0, 0)
        );
        let mut actual = [0; 8];
        local.read(0, &mut actual).unwrap();
        assert_eq!(
            actual, [0xcc; 8],
            "failed binding must not copy READ staging back"
        );
        requester.close(TIMEOUT).unwrap();
    }
}

#[test]
#[allow(unsafe_code)]
fn valid_tcp_done_without_a_source_receive_receipt_cannot_ack_or_commit_staging() {
    let runtime = crate::transport::rdma::tests::runtime("ok");
    let epoch = SourceEpoch([1; 16]);
    let registry = crate::memory::MemoryRegistry::new(epoch, 1, 8);
    let memory = registry
        .register(
            vec![0x42; 8].into_boxed_slice(),
            Permission::RemoteReadWrite,
        )
        .unwrap();
    let request = request(epoch, memory.id());
    let (mut peer, mut channel) = channels();
    channel.deadline = deadline(Duration::from_secs(1)).unwrap();
    thread::scope(|scope| {
        let handler = scope.spawn(|| runtime.serve(&mut channel, epoch, &request, &memory));
        let (paths, _) = path::read_paths(&mut peer, epoch).unwrap();
        Hello {
            requester: identity(9, 8, 7),
            source: None,
            expected_domain: paths[0].domain,
            direct: false,
            qp_width: 0,
            previous_grant: 0,
        }
        .write(&mut peer)
        .unwrap();
        assert_eq!(
            Reply::read(&mut peer, epoch).unwrap().length,
            OFFER_SIZE as u64
        );
        let mut offer = [0; OFFER_SIZE];
        peer.read_exact(&mut offer).unwrap();
        let local = Identity::decode(&offer[..IDENTITY_SIZE]).unwrap();
        let binding = Binding::new(
            &request,
            local,
            identity(9, 8, 7),
            u64_at(&offer, IDENTITY_SIZE),
            0,
        )
        .unwrap();
        binding.write(&mut peer, ACCEPT).unwrap();
        peer.write_all(&offer[IDENTITY_SIZE + 8..OFFER_SIZE - 1])
            .unwrap();
        assert_eq!(Reply::read(&mut peer, epoch).unwrap().status, 9);
        binding.read(&mut peer, GRANT).unwrap();
        let mut bytes = [0; DESCRIPTOR_SIZE];
        peer.read_exact(&mut bytes).unwrap();
        let grant = Descriptor::decode(&bytes).unwrap();
        // CPU provider control: stage bytes as though a peer had written payload.
        // No native transport ordering or DMA receipt is inferred from this write.
        unsafe {
            std::ptr::write_bytes(grant.address as *mut u8, 0xc3, 8);
        }
        binding.write(&mut peer, DONE).unwrap();
        assert_eq!(runtime.stats().unwrap().receive_work_requests, 1);
        let result = handler.join().unwrap();
        assert!(
            matches!(
                result,
                Err(Error::Timeout
                    | Error::Unquiesced {
                        operation: "RDMA close deadline before quiescence",
                        code: libc::ETIMEDOUT
                    })
            ),
            "{result:?}"
        );
    });
    // Successful ACK cannot be present, even for an exact, correctly bound DONE.
    drop(channel);
    expect_closed_without_next_phase(&mut peer);
    runtime.close(deadline(TIMEOUT).unwrap()).unwrap();
    let mut actual = [0; 8];
    memory.read(0, &mut actual).unwrap();
    assert_eq!(actual, [0x42; 8]);
    let stats = runtime.stats().unwrap();
    assert_eq!(
        (
            stats.completed_receives,
            stats.completed_window_invalidations,
            stats.receive_work_requests,
            stats.message_bytes,
            stats.active_message_mrs,
            stats.active_endpoints
        ),
        (0, 0, 0, 0, 0, 0)
    );
    runtime.close(deadline(TIMEOUT).unwrap()).unwrap();
}

#[path = "reuse.rs"]
mod reuse;

mod rails;

#[path = "cold_connections.rs"]
mod cold_connections;
