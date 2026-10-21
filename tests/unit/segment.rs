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
