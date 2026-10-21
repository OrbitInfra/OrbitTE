use super::*;

#[test]
fn missing_cuda_entry_point_returns_unavailable_without_eager_loader_panic() {
    // SAFETY: a known system DSO with no CUDA exports is a deterministic
    // missing-symbol control; it does not initialize or invoke a CUDA driver.
    let library = unsafe { Library::new("libc.so.6") }.unwrap();
    assert!(matches!(
        Driver::from_library(library),
        Err(Error::Unavailable("CUDA driver symbol cuInit"))
    ));
}
