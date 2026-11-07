use super::fabric_attrs::*;

#[cfg(feature = "fabric")]
#[path = "fabric/shim.rs"]
mod shim;
#[cfg(feature = "fabric")]
pub(super) fn calls(real: super::sys::Calls) -> super::sys::Calls {
    shim::calls(real)
}

fn peer() -> PeerAttr {
    PeerAttr {
        version: 1,
        provider: "tcp;ofi_rxm".into(),
        virt_addr: true,
        nics: vec![Nic {
            name: "lo".into(),
            addr: hex(&[0, 1, 2, 255]),
            numa: -1,
        }],
    }
}
#[test]
fn peer_attr_round_trip() {
    let p = peer();
    assert_eq!(PeerAttr::decode(&p.encode().unwrap()).unwrap(), p);
}
#[test]
fn peer_attr_rejects_bad_input() {
    let mut p = peer();
    p.version += 1;
    assert!(PeerAttr::decode(&p.encode().unwrap()).is_err());
    p = peer();
    p.nics[0].addr = "a".into();
    assert!(PeerAttr::decode(&p.encode().unwrap()).is_err());
    p.nics[0].addr.clear();
    assert!(PeerAttr::decode(&p.encode().unwrap()).is_err());
    assert!(PeerAttr::decode(b"{}").is_err());
}
#[test]
fn buffer_attr_round_trip_and_validation() {
    let mut b = BufferAttr {
        chunks: vec![
            Chunk {
                off: 0,
                len: 8,
                nics: vec![0, 1],
                keys: vec![7, 9],
            },
            Chunk {
                off: 8,
                len: 4,
                nics: vec![1],
                keys: vec![11],
            },
        ],
    };
    assert_eq!(BufferAttr::decode(&encode(&b).unwrap(), 12, 2).unwrap(), b);
    assert_eq!(b.chunks[0].key_for(1), Some(9));
    assert_eq!(b.chunks[1].key_for(0), None);
    assert_eq!(
        b.ranges(),
        vec![Range { off: 0, len: 8 }, Range { off: 8, len: 4 }]
    );
    assert!(b.validate(13, 2).is_err());
    b.chunks[1].off = 9;
    assert!(b.validate(12, 2).is_err());
    b.chunks[1].off = 8;
    b.chunks[1].keys.clear();
    assert!(b.validate(12, 2).is_err());
    b.chunks[1].keys.push(11);
    b.chunks[0].len = u64::MAX;
    assert!(b.validate(12, 2).is_err());
}
#[test]
fn plan_chunks_matches_tent_and_bounds_allocation() {
    assert_eq!(
        plan_chunks(10, 4).unwrap(),
        vec![
            Range { off: 0, len: 4 },
            Range { off: 4, len: 4 },
            Range { off: 8, len: 2 }
        ]
    );
    assert_eq!(plan_chunks(10, 0).unwrap(), vec![Range { off: 0, len: 10 }]);
    assert!(plan_chunks(0, 0).is_err());
    assert!(plan_chunks(u64::MAX, 1).is_err());
}
#[test]
fn assign_full_coverage_within_budget() {
    assert_eq!(
        assign_nics(&plan_chunks(8192, 4096).unwrap(), 2, 0, 2).unwrap(),
        vec![vec![0, 1], vec![0, 1]]
    );
    assert!(assign_nics(&[Range { off: 0, len: 1 }], 0, 4096, 0).is_err());
}
#[test]
fn assign_disjoint_partition_over_budget() {
    assert_eq!(
        assign_nics(&plan_chunks(8192, 4096).unwrap(), 5, 4096, 1).unwrap(),
        vec![vec![0, 1, 2], vec![3, 4]]
    );
    assert!(assign_nics(&[Range { off: 0, len: 8192 }], 2, 4096, 1).is_err());
}
#[test]
fn assign_round_robin_and_reject_overflow() {
    assert_eq!(
        assign_nics(&plan_chunks(16384, 4096).unwrap(), 2, 4096, 2).unwrap(),
        vec![vec![0], vec![1], vec![0], vec![1]]
    );
    assert!(assign_nics(&plan_chunks(20480, 4096).unwrap(), 2, 4096, 2).is_err());
    assert!(
        assign_nics(
            &[Range {
                off: 0,
                len: u64::MAX
            }; 2],
            2,
            1,
            1
        )
        .is_err()
    );
}
#[test]
fn cut_spans_at_both_chunk_boundaries() {
    let spans = cut_spans(
        10,
        1,
        &plan_chunks(12, 4).unwrap(),
        0,
        &plan_chunks(12, 3).unwrap(),
        0,
        16,
    )
    .unwrap();
    assert_eq!(
        spans.iter().map(|s| s.len).collect::<Vec<_>>(),
        [3, 3, 1, 2, 1]
    );
    assert_eq!(
        spans.iter().map(|s| s.off).collect::<Vec<_>>(),
        [0, 3, 6, 7, 9]
    );
}
#[test]
fn cut_spans_honours_max_span_and_bounds() {
    let r = plan_chunks(12, 0).unwrap();
    assert_eq!(cut_spans(7, 0, &r, 0, &r, 2, 4).unwrap().len(), 4);
    assert!(cut_spans(7, 0, &r, 0, &r, 2, 3).is_err());
    assert!(cut_spans(13, 0, &r, 0, &r, 0, 10).is_err());
    assert!(cut_spans(7, u64::MAX, &r, 0, &r, 0, 10).is_err());
}

#[test]
fn default_build_never_selects_or_loads_a_fabric_owner() -> crate::Result<()> {
    #[cfg(feature = "fabric")]
    let loads = super::sys::loads();
    use crate::*;
    let e = TransferEngine::new(Default::default())?;
    assert!(!e.transport_installed(Transport::Fabric));
    #[cfg(feature = "fabric")]
    assert!(e.stats()?.fabric.is_none());
    #[cfg(feature = "fabric")]
    assert_eq!(loads, super::sys::loads());
    Ok(())
}

#[test]
fn hex_round_trip_and_bad_input() {
    assert_eq!(unhex(&hex(&[0, 1, 127, 255])).unwrap(), [0, 1, 127, 255]);
    assert!(unhex("0").is_err());
    assert!(unhex("xx").is_err());
}

#[cfg(feature = "fabric")]
mod real {
    use crate::test_support::EVENTUAL_PROGRESS as WAIT;
    use crate::*;
    #[test]
    fn real_provider_guarded_read_write() -> Result<()> {
        let config = || EngineConfig {
            tcp: false,
            fabric: Some(FabricConfig {
                provider: FabricProvider::Tcp,
                devices: vec!["lo".into()],
                max_mr_size: 4096,
                slice_size: 1024,
                ..Default::default()
            }),
            ..Default::default()
        };
        let source = match TransferEngine::new(config()) {
            Ok(e) => e,
            Err(e) if std::env::var_os("ORBITE_REQUIRE_FABRIC").is_none() => {
                eprintln!("libfabric unavailable: {e}; set ORBITE_REQUIRE_FABRIC=1 to require it");
                return Ok(());
            }
            Err(e) => return Err(e),
        };
        let requester = TransferEngine::new(config())?;
        let bytes: Vec<_> = (0..16387).map(|n| (n % 251) as u8).collect();
        let remote = source.register_memory(bytes.clone().into(), Permission::RemoteReadWrite)?;
        let local =
            requester.register_memory(vec![0xee; bytes.len()].into(), Permission::LocalOnly)?;
        let peer = requester.open_segment(source.endpoint(), source.source_epoch(), WAIT)?;
        let r = TransferRequest::new(
            Transport::Fabric,
            local.base_address() + 1,
            remote.base_address() + 3,
            bytes.len() - 6,
        );
        let report =
            requester.submit_and_wait(TransferOp::Read, &peer, std::slice::from_ref(&r), WAIT)?;
        assert!(report.is_complete(), "{report:?}");
        let mut got = vec![0; bytes.len()];
        local.read(0, &mut got)?;
        assert_eq!(&got[1..bytes.len() - 5], &bytes[3..bytes.len() - 3]);
        assert_eq!(got[0], 0xee);
        assert!(got[bytes.len() - 5..].iter().all(|&b| b == 0xee));
        local.write(1, &vec![0xa5; bytes.len() - 6])?;
        let report = requester.submit_and_wait(TransferOp::Write, &peer, &[r], WAIT)?;
        assert!(report.is_complete(), "{report:?}");
        remote.read(0, &mut got)?;
        assert_eq!(&got[..3], &bytes[..3]);
        assert_eq!(&got[bytes.len() - 3..], &bytes[bytes.len() - 3..]);
        assert!(got[3..bytes.len() - 3].iter().all(|&b| b == 0xa5));
        requester.close_segment(source.endpoint(), source.source_epoch(), WAIT)?;
        requester.close(WAIT)?;
        source.close(WAIT)?;
        Ok(())
    }
}

#[cfg(feature = "fabric")]
#[path = "fabric/native.rs"]
mod native;

#[cfg(feature = "fabric")]
pub(super) fn drain_clock() -> std::sync::Arc<std::sync::atomic::AtomicBool> {
    shim::drain_clock()
}

#[cfg(feature = "fabric")]
#[path = "fabric/processes.rs"]
mod processes;

#[test]
fn native_capability_and_request_byte_fixture() {
    use crate::runtime::control_plane::{Operation, Request};
    use crate::segment::SegmentMetadata;
    let cap = SegmentMetadata {
        libfabric: true,
        ..Default::default()
    };
    let mut bytes = [0; 44];
    cap.encode(&mut bytes);
    let mut expected = [0; 44];
    expected[1] = 1;
    assert_eq!(bytes, expected);
    assert!(SegmentMetadata::decode(&bytes).unwrap().libfabric);
    bytes[1] = 2;
    assert!(SegmentMetadata::decode(&bytes).is_err());
    let request = Request {
        operation: Operation::FabricRead,
        epoch: crate::SourceEpoch([7; 16]),
        region: 9,
        offset: 2,
        length: 8,
    }
    .encode();
    assert_eq!(
        hex(&request),
        "4f524249545445000001170007070707070707070707070707070707000000000000000900000000000000020000000000000008"
    );
}
