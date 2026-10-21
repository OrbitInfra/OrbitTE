use super::*;

#[test]
fn epoch_roundtrip_rejects_malformed_authority() {
    let epoch = SourceEpoch::generate().unwrap();
    assert_eq!(epoch.to_string().parse::<SourceEpoch>().unwrap(), epoch);
    for input in [
        "",
        "1234",
        "+1000000000000000000000000000000",
        "gg000000000000000000000000000000",
        "é000000000000000000000000000000",
    ] {
        assert!(input.parse::<SourceEpoch>().is_err(), "{input}");
    }
    assert_ne!(epoch, SourceEpoch::generate().unwrap());
}

#[test]
fn address_resolution_uses_tent_containment_order_and_rejects_cross_region_ranges() {
    let epoch = SourceEpoch([1; 16]);
    let region = |id, address, length| RemoteRegion {
        epoch,
        id,
        address,
        length,
        permission: Permission::RemoteRead,
    };
    let peer = RemoteSegment {
        endpoint: "127.0.0.1:1".parse().unwrap(),
        epoch,
        regions: vec![
            region(1, 0x1004, 8),
            region(2, 0x1000, 8),
            region(3, 0x1000, 16),
            region(4, 0x1010, 8),
        ],
    };
    for address in [0x1000, 0x1004, 0x100c] {
        let (resolved, offset) = peer.resolve_address(address, 4).unwrap();
        assert_eq!((resolved.id(), offset), (3, (address - 0x1000) as usize));
    }
    assert!(matches!(
        peer.resolve_address(0x100f, 2),
        Err(Error::RegionNotFound)
    ));
    assert!(matches!(
        peer.resolve_address(u64::MAX, 2),
        Err(Error::InvalidRange)
    ));
    assert!(matches!(
        peer.resolve_address(0x1000, 0),
        Err(Error::InvalidRange)
    ));
}
