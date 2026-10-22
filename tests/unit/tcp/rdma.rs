use super::*;
use crate::engine::tests::{rdma_fixture, source_runtime};
use crate::{Permission, TransferEngine, TransferRequest};

const TIMEOUT: Duration = Duration::from_secs(3);

fn identity(epoch: u8, domain: u8, generation: u64) -> Identity {
    let mut connection = [domain; 24];
    connection[16..].copy_from_slice(&generation.to_be_bytes());
    Identity {
        epoch: SourceEpoch([epoch; 16]),
        connection: ConnectionId::decode(&connection).unwrap(),
    }
}

fn channels() -> (Channel<'static>, Channel<'static>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let sender =
        Channel::connect(listener.local_addr().unwrap(), deadline(TIMEOUT).unwrap()).unwrap();
    let receiver = Channel {
        stream: listener.accept().unwrap().0,
        deadline: deadline(TIMEOUT).unwrap(),
        stopping: None,
    };
    (sender, receiver)
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

fn send_frame(channel: &mut Channel<'_>, phase: u8, bytes: &[u8; BINDING_SIZE]) {
    channel.write_all(&[phase]).unwrap();
    channel.write_all(bytes).unwrap();
}

#[test]
fn identical_requests_on_identical_connections_cannot_replay_an_old_grant_receipt() {
    let source = identity(1, 2, 3);
    let requester = identity(4, 5, 6);
    let request = request(source.epoch, 7);
    let old = Binding::new(&request, source, requester, 1).unwrap();
    let next = Binding::new(&request, source, requester, 2).unwrap();
    for phase in [GRANT, DONE, ACK] {
        let (mut sender, mut receiver) = channels();
        old.write(&mut sender, phase).unwrap();
        assert!(matches!(
            next.read(&mut receiver, phase),
            Err(Error::Protocol(_))
        ));
    }
    assert!(Binding::new(&request, source, requester, 0).is_err());
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
) -> (Channel<'static>, Identity, u64, [u8; DESCRIPTOR_SIZE]) {
    let mut channel = Channel::connect(source.endpoint(), deadline(TIMEOUT).unwrap()).unwrap();
    channel.write_all(&request.encode()).unwrap();
    let reply = Reply::read(&mut channel, source.source_epoch()).unwrap();
    assert_eq!((reply.status, reply.length), (0, OFFER_SIZE as u64));
    let mut offer = [0; OFFER_SIZE];
    channel.read_exact(&mut offer).unwrap();
    let identity = Identity::decode(&offer[..IDENTITY_SIZE]).unwrap();
    let grant = u64_at(&offer, IDENTITY_SIZE);
    let mut descriptor = [0; DESCRIPTOR_SIZE];
    descriptor.copy_from_slice(&offer[IDENTITY_SIZE + 8..]);
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
        let binding = Binding::new(&request, local, identity(9, 8, 7), grant).unwrap();
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
        let runtime_log = crate::rdma::tests::events(source_runtime(&source));
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
        let binding = Binding::new(&request, local, identity(9, 8, 7), grant).unwrap();
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
            let mut channel = Channel {
                stream: listener.accept().unwrap().0,
                deadline: deadline(TIMEOUT).unwrap(),
                stopping: None,
            };
            let mut bytes = [0; REQUEST_SIZE];
            channel.read_exact(&mut bytes).unwrap();
            let wire = Request::decode(&bytes).unwrap();
            let payload = vec![0x42u8; 8].into_boxed_slice();
            let descriptor = grant_descriptor(payload.as_ptr() as u64);
            let local = identity(7, 8, 9);
            let grant = 1u64;
            let mut offer = [0; OFFER_SIZE];
            offer[..IDENTITY_SIZE].copy_from_slice(&local.encode());
            offer[IDENTITY_SIZE..IDENTITY_SIZE + 8].copy_from_slice(&grant.to_be_bytes());
            offer[IDENTITY_SIZE + 8..].copy_from_slice(&descriptor.connection_bytes());
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
                return;
            }
            let binding = Binding::accept(&mut channel, &wire, local, grant).unwrap();
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
        assert!(report.failure.is_some(), "stage {stage} field {field}");
        assert!(report.locally_drained);
        let stats = requester.stats().unwrap().rdma.unwrap();
        assert_eq!(
            (stats.completed_reads, stats.posted_work_requests),
            if stage == 2 { (1, 1) } else { (0, 0) }
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
