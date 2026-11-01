use super::*;

fn path(id: usize, name: &str, kind: GidType, gid: &str) -> NicPath {
    NicPath {
        nic_id: id,
        name: name.into(),
        domain: [id as u8 + 1; 16],
        gid_type: kind,
        descriptor: Descriptor {
            lid: 1,
            mtu: 3,
            link_layer: if kind == GidType::Ib { 1 } else { 2 },
            gid: gid.parse::<Ipv6Addr>().unwrap().octets(),
            ..Default::default()
        },
    }
}

#[test]
fn rail_mask_keeps_family_type_and_name_affinity_without_compacting_ids() {
    let local = vec![
        path(0, "a", GidType::Ib, "::"),
        path(2, "b", GidType::RoceV2, "::ffff:10.0.0.1"),
        path(5, "c", GidType::RoceV2, "2001:db8::1"),
    ];
    let remote = vec![
        path(0, "c", GidType::RoceV2, "2001:db8::2"),
        path(1, "x", GidType::RoceV2, "::ffff:10.0.0.2"),
    ];
    assert_eq!(local_device_mask(&local, &remote), 1 << 5);
    assert_eq!(remote_path(&local[2], &remote).unwrap().name, "c");
    assert!(!compatible(&local[1], &remote[0]));
    let v1 = path(3, "b", GidType::RoceV1, "::ffff:10.0.0.3");
    assert!(!compatible(&local[1], &v1));
    assert_eq!(local_device_mask(&local, &[v1]), 0);
    assert_eq!(local_device_mask(&local, &remote[1..]), 1 << 2);
    let preferred = vec![
        path(7, "remote-near", GidType::RoceV2, "::ffff:10.0.0.2"),
        path(1, "remote-far", GidType::RoceV2, "::ffff:10.0.0.3"),
    ];
    assert_eq!(remote_path(&local[1], &preferred).unwrap().nic_id, 7);
    let mut matched = preferred;
    matched[1].name = local[1].name.clone();
    assert_eq!(remote_path(&local[1], &matched).unwrap().nic_id, 1);
}

#[test]
fn path_codec_rejects_authority_invalid_ids_names_domains_and_link_profiles() {
    let path = path(63, "mlx5_7", GidType::RoceV2, "2001:db8::1");
    let wire = path.encode().unwrap();
    let decoded = NicPath::decode(&wire).unwrap();
    assert_eq!(decoded.nic_id, 63);
    assert_eq!(decoded.name, path.name);
    assert!(decoded.matches_connection(&path.descriptor));
    for field in [0, 1, 129, 130, 146, 147, 155, 159, 163, 169, 170, 171] {
        let mut bad = wire;
        match field {
            0 => bad[0] = 64,
            1 => bad[1] = 129,
            130 => bad[130..146].fill(0),
            146 => bad[146] = 9,
            169 => bad[169] = 6,
            170 => bad[170] = 1,
            171 => bad[171..].fill(0),
            _ => bad[field] ^= 1,
        }
        assert!(NicPath::decode(&bad).is_err(), "wire field {field}");
    }
    assert!(NicPath::decode(&wire[..186]).is_err());
    let mut changed = path.descriptor;
    changed.gid[15] ^= 1;
    assert!(!path.matches_connection(&changed));
}

#[test]
fn path_table_bounds_and_duplicate_identities_are_rejected_before_selection() {
    use std::net::TcpListener;
    use std::time::{Duration, Instant};
    let epoch = SourceEpoch([1; 16]);
    let channels = || {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let sender = ControlChannel::connect(
            listener.local_addr().unwrap(),
            Instant::now() + Duration::from_secs(1),
        )
        .unwrap();
        let receiver = ControlChannel {
            stream: listener.accept().unwrap().0,
            deadline: Instant::now() + Duration::from_secs(1),
            stopping: None,
            nonblocking: false,
        };
        (sender, receiver)
    };
    for length in [0, 1, 187, 189, 65 * PATH_SIZE as u64, u64::MAX] {
        let (mut sender, mut receiver) = channels();
        sender.reply(epoch, 0, length).unwrap();
        assert!(matches!(
            read_paths(&mut receiver, epoch),
            Err(Error::Protocol(_))
        ));
    }
    for duplicate in ["id", "name", "domain", "none"] {
        let first = path(2, "a", GidType::RoceV2, "2001:db8::1");
        let mut second = path(63, "b", GidType::RoceV2, "2001:db8::2");
        match duplicate {
            "id" => second.nic_id = first.nic_id,
            "name" => second.name.clone_from(&first.name),
            "domain" => second.domain = first.domain,
            _ => {}
        }
        let (mut sender, mut receiver) = channels();
        write_paths(&mut sender, epoch, &[first, second], 3).unwrap();
        let result = read_paths(&mut receiver, epoch);
        assert_eq!(result.is_ok(), duplicate == "none", "{duplicate}");
        if let Ok((paths, width)) = result {
            assert_eq!(width, 3);
            assert_eq!(paths.iter().map(|p| p.nic_id).collect::<Vec<_>>(), [2, 63]);
        }
    }
}
