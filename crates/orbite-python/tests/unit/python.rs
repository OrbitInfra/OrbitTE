use super::*;

#[test]
fn panic_barrier_returns_internal_error_and_unknown_effect_is_typed() {
    Python::initialize();
    Python::attach(|py| {
        let err = run::<()>(py, "panic control", || panic!("boundary control")).unwrap_err();
        assert!(err.is_instance_of::<InternalError>(py));
        let err = run::<()>(py, "uncertain", || Err(UNKNOWN)).unwrap_err();
        assert!(err.is_instance_of::<InternalError>(py));
        assert!(err.is_instance_of::<TentException>(py));
    });
}

#[test]
fn typed_exception_map_matches_the_frozen_throw_status() {
    Python::initialize();
    Python::attach(|py| {
        assert!(error(INVALID, "arg").is_instance_of::<InvalidArgumentError>(py));
        assert!(error(ADDRESS, "address").is_instance_of::<AddressNotRegisteredError>(py));
        assert!(error(Code(4), "device").is_instance_of::<DeviceNotFoundError>(py));
        assert!(error(Code(5), "entry").is_instance_of::<InvalidEntryError>(py));
        assert!(error(Code(100), "rdma").is_instance_of::<RdmaError>(py));
        assert!(error(Code(101), "cuda").is_instance_of::<CudaError>(py));
        assert!(error(Code(131), "metadata").is_instance_of::<MetadataError>(py));
        assert!(error(Code(132), "rpc").is_instance_of::<RpcServiceError>(py));
        assert!(error(UNSENT, "refused").is_instance_of::<RpcServiceError>(py));
        assert!(error(UNSUPPORTED, "unsupported").is_instance_of::<NotImplementedError>(py));
        assert!(error(FULL, "capacity").is_instance_of::<InternalError>(py));
    });
}
