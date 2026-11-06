use super::*;
use std::mem::{align_of, offset_of, size_of};

#[test]
fn versioned_lp64_records_match_the_installed_cpp_client() {
    assert_eq!((size_of::<RequestV1>(), align_of::<RequestV1>()), (88, 8));
    assert_eq!(
        [
            offset_of!(RequestV1, request),
            offset_of!(RequestV1, policy_name),
            offset_of!(RequestV1, policy_name_len),
            offset_of!(RequestV1, deadline_ns),
            offset_of!(RequestV1, intent_type),
            offset_of!(RequestV1, reserved)
        ],
        [8, 56, 64, 72, 80, 84]
    );
    assert_eq!((size_of::<Notice>(), size_of::<TaskV1>()), (32, 24));
}
