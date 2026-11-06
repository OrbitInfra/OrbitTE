use super::*;
use std::mem::{align_of, offset_of, size_of};

#[test]
fn dlpack_lp64_layout_matches_the_published_c_header() {
    assert_eq!((size_of::<Device>(), size_of::<Dtype>()), (8, 4));
    assert_eq!((size_of::<Tensor>(), align_of::<Tensor>()), (48, 8));
    assert_eq!(offset_of!(Tensor, device), 8);
    assert_eq!(offset_of!(Tensor, ndim), 16);
    assert_eq!(offset_of!(Tensor, dtype), 20);
    assert_eq!(offset_of!(Tensor, shape), 24);
    assert_eq!(offset_of!(Tensor, strides), 32);
    assert_eq!(offset_of!(Tensor, offset), 40);
    assert_eq!((size_of::<Legacy>(), align_of::<Legacy>()), (64, 8));
    assert_eq!(offset_of!(Legacy, manager), 48);
    assert_eq!(offset_of!(Legacy, deleter), 56);
    assert_eq!((size_of::<Versioned>(), align_of::<Versioned>()), (80, 8));
    assert_eq!(offset_of!(Versioned, manager), 8);
    assert_eq!(offset_of!(Versioned, deleter), 16);
    assert_eq!(offset_of!(Versioned, flags), 24);
    assert_eq!(offset_of!(Versioned, tensor), 32);
}
