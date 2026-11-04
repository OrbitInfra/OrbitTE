use super::*;
#[test]
fn describe_fixture_binds_handle_kind_domain_and_egm_location() {
    let cap = Capability {
        domain: SourceEpoch([0x53; 16]),
        kind: MnnvlHandleType::Fabric,
        egm: true,
    };
    let sample = include_bytes!("../../reference/mnnvl/describe.bin");
    let mut out = [0; 28];
    cap.encode(&mut out);
    out[24..].copy_from_slice(&Location::wire(Some(Location::Host(3))).to_be_bytes());
    assert_eq!(&out, sample);
    assert_eq!(Capability::decode(sample).unwrap(), cap);
    let word = u32::from_be_bytes(out[24..].try_into().unwrap());
    assert_eq!(
        Location::decode(word, MemoryType::Cpu, Some(cap)).unwrap(),
        Some(Location::Host(3))
    );
    for index in [16, 17, 18, 23] {
        let mut bad = out;
        bad[index] = 0xff;
        assert!(Capability::decode(&bad).is_err())
    }
    assert!(Capability::decode(&out[..23]).is_err());
    assert!(Location::decode(word, MemoryType::Cuda, Some(cap)).is_err());
    assert!(Location::decode(word, MemoryType::Cpu, None).is_err());
    let other = Capability {
        domain: SourceEpoch([1; 16]),
        ..cap
    };
    assert!(!cap.reaches(other, true, Location::Device));
    let posix = Capability {
        kind: MnnvlHandleType::PosixFd,
        ..cap
    };
    assert!(!posix.reaches(posix, false, Location::Device));
    assert!(cap.reaches(cap, false, Location::Device));
}
