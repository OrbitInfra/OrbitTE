use super::*;

#[test]
fn missing_cuda_entry_point_returns_unavailable_without_eager_loader_panic() {
    // SAFETY: a known system DSO with no CUDA exports is a deterministic
    // missing-symbol control; it does not initialize or invoke a CUDA driver.
    let library = unsafe { Library::new("libc.so.6") }.unwrap();
    assert!(batch_copy(&library).is_none());
    assert!(matches!(
        Driver::from_library(library),
        Err(Error::Unavailable("CUDA driver symbol cuInit"))
    ));
}

#[test]
fn native_batch_probe_requires_supported_version_and_exact_v2_abi() {
    let directory =
        std::env::temp_dir().join(format!("orbitte-batch-probe-{}", std::process::id()));
    std::fs::create_dir_all(&directory).unwrap();
    for old_only in [false, true] {
        let output = directory.join(if old_only { "old.so" } else { "v2.so" });
        let source =
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/unit/cuda/batch_probe.c");
        let mut command = std::process::Command::new("cc");
        command.args([
            "-std=c11", "-shared", "-fPIC", "-Wall", "-Wextra", "-Werror",
        ]);
        if old_only {
            command.arg("-DOLD_ONLY");
        }
        assert!(
            command
                .arg(source)
                .arg("-o")
                .arg(&output)
                .status()
                .unwrap()
                .success()
        );
        // SAFETY: this locally compiled fixture exports the declared C ABI.
        unsafe {
            let library = Library::new(&output).unwrap();
            let set = library
                .get::<unsafe extern "C" fn(c_int)>(b"set_version\0")
                .unwrap();
            for version in [-1, 12080, 12090, 13000, 13010] {
                set(version);
                let call = batch_copy(&library);
                assert_eq!(call.is_some(), version >= 13000 && !old_only);
                if let Some(call) = call {
                    let location = sys::CUmemLocation {
                        type_: sys::CUmemLocationType::CU_MEM_LOCATION_TYPE_INVALID,
                        id: 0,
                    };
                    let mut attrs = sys::CUmemcpyAttributes {
                        srcAccessOrder:
                            sys::CUmemcpySrcAccessOrder::CU_MEMCPY_SRC_ACCESS_ORDER_STREAM,
                        srcLocHint: location,
                        dstLocHint: location,
                        flags: 0,
                    };
                    assert_eq!(
                        call(
                            &mut 100,
                            &mut 200,
                            &mut 17,
                            1,
                            &mut attrs,
                            &mut 0,
                            1,
                            3usize as sys::CUstream
                        ),
                        SUCCESS
                    );
                }
            }
        }
    }
    std::fs::remove_dir_all(directory).unwrap();
}
