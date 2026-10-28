use super::*;
use crate::engine::deadline;
use crate::runtime::control_plane::{ControlChannel, Operation, Reply, Request, u64_at};
use protocol::*;
use std::net::TcpListener;

const TIMEOUT: Duration = Duration::from_secs(2);

fn identity(epoch: SourceEpoch, domain: u8) -> Identity {
    let mut generation = 1;
    Identity {
        epoch,
        connection: ConnectionId::next([domain; 16], &mut generation).unwrap(),
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

pub(crate) fn check_source(runtime: &RdmaTransport, epoch: SourceEpoch, memory: &RegisteredMemory) {
    let request = Request {
        operation: Operation::RdmaRead,
        epoch,
        region: memory.id(),
        offset: 0,
        length: 8,
    };
    for case in ["private", "width", "domain", "cached"] {
        let (mut peer, mut channel) = channels();
        std::thread::scope(|scope| {
            let handler = scope.spawn(|| runtime.serve(&mut channel, epoch, &request, memory));
            let (paths, width) = path::read_paths(&mut peer, epoch).unwrap();
            assert_eq!(width, 0, "GPU source cannot advertise reusable QPs");
            Hello {
                requester: identity(SourceEpoch([5; 16]), 6),
                source: (case == "cached").then(|| {
                    let mut source = identity(epoch, 7);
                    source.connection.domain = paths[0].domain;
                    source
                }),
                expected_domain: if case == "domain" {
                    [9; 16]
                } else {
                    paths[0].domain
                },
                direct: true,
                qp_width: usize::from(case == "width"),
                previous_grant: u64::from(case == "cached"),
            }
            .write(&mut peer)
            .unwrap();
            if case == "private" {
                assert_eq!(
                    Reply::read(&mut peer, epoch).unwrap().length,
                    OFFER_SIZE as u64
                );
                let mut offer = [0; OFFER_SIZE];
                peer.read_exact(&mut offer).unwrap();
                let source = Identity::decode(&offer[..IDENTITY_SIZE]).unwrap();
                assert_eq!(source.epoch, epoch);
                assert_ne!(source.connection.domain, paths[0].domain);
                assert_eq!(offer[OFFER_SIZE - 1], MR_AUTHORITY);
                assert_ne!(u64_at(&offer, IDENTITY_SIZE), 0);
            } else if case == "cached" {
                assert_eq!(Reply::read(&mut peer, epoch).unwrap().status, CACHE_MISS);
            }
            drop(peer);
            let result = handler.join().unwrap();
            if matches!(case, "width" | "domain") {
                assert!(
                    matches!(result, Err(Error::Protocol(_))),
                    "{case}: {result:?}"
                );
            } else if case == "cached" {
                assert!(
                    result.is_ok(),
                    "cached owner must be rejected without native construction"
                );
            } else {
                assert!(
                    result.is_err(),
                    "closing before ACCEPT must abort the private offer"
                );
            }
        });
        let stats = runtime.stats().unwrap();
        assert_eq!(
            (
                stats.active_grants,
                stats.active_grant_pds,
                stats.active_endpoints
            ),
            (0, 0, 0),
            "{case}"
        );
        assert_eq!(
            stats.opened_grant_pds, 1,
            "invalid/reused metadata must not create a private PD"
        );
    }
}

#[test]
fn requester_accepts_private_domain_only_without_reusable_qp_ownership() {
    use crate::engine::tests::rdma_fixture;
    use crate::{Permission, RemoteRegion, RemoteSegment, TransferRequest, Transport};
    for (profile, width, accepts) in [
        (MR_AUTHORITY, 0, true),
        (MR_AUTHORITY, 1, false),
        (0, 0, false),
    ] {
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
                permission: Permission::RemoteRead,
            }],
        };
        let handler = std::thread::spawn(move || {
            let mut channel = ControlChannel {
                stream: listener.accept().unwrap().0,
                deadline: deadline(TIMEOUT).unwrap(),
                stopping: None,
            };
            let request = crate::runtime::control_plane::tests::read_request(&mut channel);
            let descriptor = Descriptor {
                qpn: 42,
                psn: 17,
                lid: 1,
                mtu: 3,
                link_layer: 1,
                ..Default::default()
            };
            path::write_paths(
                &mut channel,
                epoch,
                &[path::NicPath {
                    nic_id: 0,
                    name: "copy".into(),
                    domain: [8; 16],
                    gid_type: discovery::GidType::Ib,
                    descriptor: Descriptor {
                        qpn: 0,
                        psn: 0,
                        ..descriptor
                    },
                }],
                width,
            )
            .unwrap();
            let hello = Hello::read(&mut channel, epoch).unwrap();
            let local = identity(epoch, 9);
            let mut offer = [0; OFFER_SIZE];
            offer[..IDENTITY_SIZE].copy_from_slice(&local.encode());
            offer[IDENTITY_SIZE..IDENTITY_SIZE + 8].copy_from_slice(&1u64.to_be_bytes());
            offer[IDENTITY_SIZE + 8..OFFER_SIZE - 1]
                .copy_from_slice(&descriptor.connection_bytes());
            offer[OFFER_SIZE - 1] = profile;
            channel.reply(epoch, 0, OFFER_SIZE as u64).unwrap();
            channel.write_all(&offer).unwrap();
            let result =
                Binding::accept(&mut channel, &request, local, hello.requester, 1, profile);
            assert_eq!(result.is_ok(), accepts, "profile {profile}, width {width}");
            if accepts {
                channel.read_exact(&mut [0; DESCRIPTOR_SIZE]).unwrap();
            }
        });
        let engine = rdma_fixture("direct:copy");
        let memory = engine
            .register_memory(vec![0; 8].into_boxed_slice(), Permission::LocalOnly)
            .unwrap();
        let report = engine
            .submit_and_wait(
                TransferOp::Read,
                &peer,
                &[TransferRequest {
                    transport: Transport::Rdma,
                    local_address: memory.base_address(),
                    remote_address: 0x1000,
                    length: 8,
                }],
                TIMEOUT,
            )
            .unwrap();
        handler.join().unwrap();
        assert!(!report.is_complete());
        if !accepts {
            assert!(matches!(report.failure.unwrap().error, Error::Protocol(_)));
        }
        engine.close(TIMEOUT).unwrap();
    }
}
