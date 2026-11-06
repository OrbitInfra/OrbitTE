use super::*;
use crate::{TransferRequest, Transport};
use std::path::Path;
use std::process::Command;

// Run alone so other tests' threads, driver singletons and rings cannot affect the count.
#[test]
fn storage_runtime_isolation() -> Result<()> {
    const CHILD: &str = "ORBITE_TEST_STORAGE_AUDIT";
    if std::env::var_os(CHILD).is_some() {
        return exercise();
    }
    let temp = TempFile::new(b"");
    let fixtures = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/gds");
    let build = |src: &str, output: &Path| -> Result<()> {
        assert!(
            Command::new("cc")
                .args(["-shared", "-fPIC", "-Wall", "-Werror"])
                .arg(fixtures.join(src))
                .arg("-o")
                .arg(output)
                .status()?
                .success()
        );
        Ok(())
    };
    let audit = temp.dir.join("audit.so");
    build("storage_audit.c", &audit)?;
    let shim = temp.dir.join("libcufile.so.0");
    let mut providers = vec![(None, "enabled")];
    if cfg!(feature = "gds") {
        build("cufile_shim.c", &shim)?;
        providers.extend([(Some(&shim), "enabled"), (Some(&shim), "disabled")]);
        if cfg!(feature = "io-uring") {
            providers.push((Some(&shim), "io-only"));
        }
    }
    for (provider, profile) in providers {
        let mut cmd = Command::new(std::env::current_exe()?);
        cmd.args([
            "--exact",
            "--nocapture",
            "segment::file::tests::isolation::storage_runtime_isolation",
        ])
        .env(CHILD, profile)
        .env("LD_AUDIT", &audit)
        .env_remove("ORBITE_TEST_CUFILE");
        if let Some(provider) = provider {
            cmd.env("ORBITE_TEST_CUFILE", provider)
                .env("ORBITE_CUFILE_SHIM", "native");
        }
        let output = cmd.output()?;
        let stderr = String::from_utf8_lossy(&output.stderr);
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(
            output.status.success() && stdout.contains("1 passed"),
            "{stdout}\n{stderr}"
        );
        assert!(stderr.contains("storage audit active"), "{stderr}");
        assert_eq!(
            stderr.contains("cuFile lookup"),
            cfg!(feature = "gds") && profile == "enabled",
            "{stderr}"
        );
        if let Some(index) = stderr.find("cuFile lookup") {
            assert!(
                stderr
                    .find("first file submission")
                    .is_some_and(|start| start < index),
                "{stderr}"
            );
        }
        assert!(!stderr.contains("liburing lookup"), "{stderr}");
    }
    Ok(())
}

fn live_rings() -> Result<usize> {
    let mut count = 0;
    for fd in fs::read_dir("/proc/self/fd")? {
        let target = fs::read_link(fd?.path())?;
        count += usize::from(target == Path::new("anon_inode:[io_uring]"));
    }
    Ok(count)
}

fn exercise() -> Result<()> {
    let temp = TempFile::new(b"file");
    let threads = fs::read_dir("/proc/self/task")?.count();
    let profile = std::env::var("ORBITE_TEST_STORAGE_AUDIT").unwrap();
    let engine = TransferEngine::new(EngineConfig {
        selector: (profile == "io-only")
            .then(|| {
                crate::TransportSelectorConfig::from_json(
                    r#"{"policy":[{"segment_type":"file","transports":["io_uring"]}]}"#,
                )
            })
            .transpose()?,
        handler_threads: 1,
        #[cfg(feature = "gds")]
        gds_enabled: profile != "disabled",
        // Count only the control and storage owners in this isolation profile.
        #[cfg(feature = "metrics")]
        metrics: crate::metrics::MetricsConfig {
            enabled: false,
            ..Default::default()
        },
        ..Default::default()
    })?;
    let check_idle = || -> Result<()> {
        assert_eq!(live_rings()?, 0);
        // The control acceptor and its one handler account for all new threads.
        assert_eq!(fs::read_dir("/proc/self/task")?.count(), threads + 2);
        Ok(())
    };
    check_idle()?;
    let file = engine.open_file_segment(&temp.uri(), Permission::RemoteReadWrite)?;
    let info = engine.file_segment_info(&file)?;
    assert_eq!(info.length, 4);
    check_idle()?;
    #[cfg(any(feature = "io-uring", feature = "gds"))]
    assert_eq!(info.direct_io, None);
    let has_gds = cfg!(feature = "gds")
        && std::env::var_os("ORBITE_TEST_CUFILE").is_some()
        && profile == "enabled";
    #[cfg(feature = "gds")]
    {
        assert_eq!(engine.stats()?.gds, crate::GdsStats::default());
        assert_eq!(info.gds_mode, None);
    }
    let memory = engine.register_memory(b"host".to_vec().into(), Permission::LocalOnly)?;
    #[cfg(feature = "io-uring")]
    assert_eq!(
        crate::transport::io_uring::tests::RING_SETUPS.load(std::sync::atomic::Ordering::Relaxed),
        0
    );
    eprintln!("first file submission");
    #[cfg(feature = "io-uring")]
    let has_ring = crate::transport::io_uring::tests::kernel();
    #[cfg(not(feature = "io-uring"))]
    let has_ring = false;
    for hint in [Transport::Gds, Transport::Unspec] {
        let request = [TransferRequest::new(hint, memory.base_address(), 0, 4)];
        for op in [TransferOp::Read, TransferOp::Write] {
            memory.write(0, b"host")?;
            let report = engine.submit_and_wait(op, &file, &request, Duration::from_secs(5));
            if has_gds || (hint == Transport::Unspec && has_ring) {
                assert!(report?.is_complete());
                let mut bytes = [0; 4];
                memory.read(0, &mut bytes)?;
                assert_eq!(
                    &bytes,
                    if op == TransferOp::Read && !(has_gds && hint == Transport::Unspec) {
                        b"file"
                    } else {
                        b"host"
                    }
                );
            } else if profile == "io-only" {
                assert!(matches!(report, Err(Error::TransportPolicy { .. })));
            } else {
                assert!(matches!(report, Err(Error::Unavailable(_))));
            }
        }
    }
    assert_eq!(
        fs::read(&temp.path)?,
        if has_ring || has_gds {
            b"host"
        } else {
            b"file"
        }
    );
    assert_eq!(live_rings()?, 0);
    #[cfg(feature = "io-uring")]
    if crate::transport::io_uring::tests::kernel() {
        // Positive control for the fd inventory.
        assert!(
            has_ring,
            "a live kernel must make the engine's io_uring available"
        );
        let _ring = ::io_uring::IoUring::new(1)?;
        assert_eq!(live_rings()?, 1);
    }
    engine.close_file_segment(&file, Duration::ZERO)
}
