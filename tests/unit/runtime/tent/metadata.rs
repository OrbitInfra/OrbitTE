use super::*;

fn fixture() -> serde_json::Value {
    let rpc: serde_json::Value =
        serde_json::from_str(include_str!("../../../reference/wire/rpc.json")).unwrap();
    serde_json::from_str(
        rpc["GetSegmentDesc"]["response"]["attachment"]
            .as_str()
            .unwrap(),
    )
    .unwrap()
}

#[test]
fn frozen_metadata_is_retained_as_a_restart_signal_without_inventing_an_epoch() {
    let mut value = fixture();
    let endpoint = "127.0.0.1:19000".parse().unwrap();
    let epoch = SourceEpoch::generate().unwrap();
    let peer = decode(&serde_json::to_vec(&value).unwrap(), endpoint, epoch).unwrap();
    assert!(peer.regions.is_empty()); // fixture advertises HP-TCP and RDMA, not TCP.
    value["detail"]["buffers"][0]["transports"] = serde_json::json!([7]);
    let peer = decode(&serde_json::to_vec(&value).unwrap(), endpoint, epoch).unwrap();
    assert_eq!(peer.epoch, epoch);
    assert_eq!(peer.regions[0].address, 0x0102030405060708);
    assert_eq!(peer.regions[0].permission, Permission::RemoteReadWrite);
    let snapshot = peer.metadata.tent.unwrap();
    assert_eq!(snapshot["detail"]["buffers"], value["detail"]["buffers"]);
    value["orbite_source_epoch"] = epoch.to_string().into();
    assert!(matches!(
        decode(&serde_json::to_vec(&value).unwrap(), endpoint, epoch),
        Err(Error::Unavailable(_))
    ));
}

#[test]
fn metadata_rejects_redirects_overflow_and_excluded_memory_and_bootstrap_matches_fixture() {
    let endpoint = "127.0.0.1:19000".parse().unwrap();
    let epoch = SourceEpoch::generate().unwrap();
    for (key, bad) in [
        ("addr", serde_json::json!(u64::MAX)),
        ("length", serde_json::json!(0)),
        ("location", serde_json::json!("rocm:0")),
    ] {
        let mut v = fixture();
        v["detail"]["buffers"][0]["transports"] = serde_json::json!([7]);
        v["detail"]["buffers"][0][key] = bad;
        assert!(decode(&serde_json::to_vec(&v).unwrap(), endpoint, epoch).is_err());
    }
    let mut v = fixture();
    v["rpc_server_addr"] = "192.0.2.1:1".into();
    assert!(matches!(
        decode(&serde_json::to_vec(&v).unwrap(), endpoint, epoch),
        Err(Error::Protocol(_))
    ));
    let rpc: serde_json::Value =
        serde_json::from_str(include_str!("../../../reference/wire/rpc.json")).unwrap();
    let b: serde_json::Value = serde_json::from_str(
        rpc["BootstrapRdma"]["request"]["attachment"]
            .as_str()
            .unwrap(),
    )
    .unwrap();
    let decoded: Bootstrap = serde_json::from_value(b.clone()).unwrap();
    assert_eq!(serde_json::to_value(decoded).unwrap(), b);
}

#[test]
fn snapshot_ignores_numeric_map_order_and_topology_whitespace_but_preserves_values() {
    let epoch = SourceEpoch::generate().unwrap();
    let endpoint = "127.0.0.1:19000".parse().unwrap();
    let mut value = fixture();
    value["detail"]["transport_attrs"] = serde_json::json!([[7, "tcp"], [1, "rdma"]]);
    let old = decode(&serde_json::to_vec(&value).unwrap(), endpoint, epoch).unwrap();
    value["detail"]["transport_attrs"]
        .as_array_mut()
        .unwrap()
        .reverse();
    let topology: serde_json::Value =
        serde_json::from_str(value["detail"]["topology"].as_str().unwrap()).unwrap();
    value["detail"]["topology"] = serde_json::to_string(&topology).unwrap().into();
    let same = decode(&serde_json::to_vec(&value).unwrap(), endpoint, epoch).unwrap();
    assert_eq!(old.metadata.tent, same.metadata.tent);
    value["detail"]["transport_attrs"][0][1] = "changed".into();
    let changed = decode(&serde_json::to_vec(&value).unwrap(), endpoint, epoch).unwrap();
    assert_ne!(old.metadata.tent, changed.metadata.tent);
    let formats: serde_json::Value =
        serde_json::from_str(include_str!("../../../reference/wire/formats.json")).unwrap();
    assert!(matches!(
        decode(
            &serde_json::to_vec(&formats["file"]).unwrap(),
            endpoint,
            epoch
        ),
        Err(Error::Unavailable(_))
    ));
}
