use super::abi::*;
#[test]
fn records_match_frozen_lp64() {
    assert_eq!(size_of::<Location>(), 48);
    assert_eq!(align_of::<Location>(), 8);
    assert_eq!(size_of::<Table>(), 72);
    assert_eq!(align_of::<Table>(), 8);
    assert_eq!(
        DESCRIPTOR.offsets,
        [0, 8, 16, 24, 32, 40, 48, 56, 64, 0, 8, 16]
    );
    assert_eq!(HANDLED_ERROR, -3);
    assert!(!Table::default().complete());
}
