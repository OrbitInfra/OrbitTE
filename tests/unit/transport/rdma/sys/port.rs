use super::*;

#[test]
fn negotiated_speed_decodes_xdr_and_leaves_unknown_values_unset() {
    for (speed, width, expected) in [
        (1, 1, Some(2500)),
        (4, 2, Some(40000)),
        (16, 4, Some(112000)),
        (32, 8, Some(300000)),
        (64, 2, Some(200000)),
        (128, 2, Some(400000)),
        (128, 16, Some(200000)),
        (256, 2, Some(800000)),
        (256, 1, Some(200000)),
        (512, 2, None),
        (0, 2, None),
        (3, 2, None),
        (128, 0, None),
        (128, 3, None),
    ] {
        let attr = shim::orbite_port_attr {
            active_speed: speed,
            active_width: width,
            ..Default::default()
        };
        assert_eq!(link_speed_mbps(&attr), expected, "{speed}/{width}");
    }
}

#[test]
fn public_header_port_query_and_optional_symbol_preserve_fields_errors_and_units() {
    use std::process::Command;
    let dir = std::env::temp_dir().join(format!(
        "orbite-port-abi-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir(&dir).unwrap();
    let source = dir.join("port_abi.c");
    std::fs::write(&source, include_str!("port_abi.c")).unwrap();
    for present in [false, true] {
        let library = dir.join(if present { "new.so" } else { "old.so" });
        let mut command = Command::new(std::env::var_os("CC").unwrap_or_else(|| "cc".into()));
        command.args(["-shared", "-fPIC", "-Wall", "-Werror"]);
        if present {
            command.arg("-DORBITE_WITH_PORT_SPEED");
        }
        if env!("ORBITE_RDMA_ACTIVE_SPEED_EX") == "true" {
            command.arg("-DORBITE_HAVE_ACTIVE_SPEED_EX");
        }
        let output = command
            .arg(&source)
            .arg("-o")
            .arg(&library)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        // SAFETY: The test loads its freshly compiled fixture library, whose initializer and ABI
        // it controls.
        let library = unsafe { Library::new(&library) }.unwrap();
        // SAFETY: The fixture exports this exact C signature, and the Library outlives its
        // symbol.
        let make_context = unsafe {
            library.get::<unsafe extern "C" fn(c_int) -> *mut ffi::ibv_context>(
                b"orbite_test_context\0",
            )
        }
        .unwrap();
        for mode in 0..3 {
            // SAFETY: The typed fixture symbol is loaded from the live library and returns its
            // own context.
            let context = unsafe { make_context(mode) };
            // SAFETY: The fixture library owns the returned verbs context and stays loaded during
            // the port query.
            let result = unsafe { query_port(context, 7) };
            if mode == 2 {
                assert!(matches!(result, Err(libc::EIO)));
                continue;
            }
            let attr = result.unwrap();
            assert_eq!((attr.state, attr.active_mtu, attr.gid_tbl_len), (4, 5, 17));
            assert_eq!((attr.lid, attr.link_layer, attr.active_width), (42, 2, 2));
            let expected = if mode == 0 && env!("ORBITE_RDMA_ACTIVE_SPEED_EX") == "true" {
                800_000
            } else {
                400_000
            };
            assert_eq!(link_speed_mbps(&attr), Some(expected));
        }
        let query = PortSpeedQuery::from_library(library);
        assert_eq!(query.is_some(), present);
        if let Some(query) = query {
            for (port, expected) in [(1, 200_000), (4, 0)] {
                assert_eq!(
                    // SAFETY: This fixture symbol explicitly accepts a null context and remains
                    // loaded by the query owner.
                    unsafe { effective_speed(query.query, std::ptr::null_mut(), port) }.unwrap(),
                    expected
                );
            }
            for (port, code) in [(2, libc::EIO), (3, libc::EOVERFLOW)] {
                assert!(
                    // SAFETY: This fixture symbol explicitly accepts a null context and remains
                    // loaded by the query owner.
                    matches!(unsafe { effective_speed(query.query,std::ptr::null_mut(),port) },Err(Error::Backend {code:observed,..}) if observed==code)
                );
            }
        }
    }
    std::fs::remove_dir_all(dir).unwrap();
}
