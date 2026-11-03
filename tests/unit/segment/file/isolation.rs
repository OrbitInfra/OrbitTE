use super::*;
use crate::{TransferRequest, Transport};
use std::path::Path;
use std::process::Command;

// Run alone so other tests' threads, driver singletons and rings cannot affect the count.
#[test]
fn storage_runtime_isolation() -> Result<()> {
    const CHILD: &str = "ORBITTE_TEST_STORAGE_AUDIT";
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
    let mut providers = vec![None];
    if cfg!(feature = "gds") {
        build("cufile_shim.c", &shim)?;
        providers.push(Some(&shim));
    }
    for provider in providers {
        let mut cmd = Command::new(std::env::current_exe()?);
        cmd.args([
            "--exact",
            "segment::file::tests::isolation::storage_runtime_isolation",
        ])
        .env(CHILD, "1")
        .env("LD_AUDIT", &audit)
        .env_remove("ORBITTE_TEST_CUFILE");
        if let Some(provider) = provider {
            cmd.env("ORBITTE_TEST_CUFILE", provider)
                .env("ORBITTE_CUFILE_SHIM", "native");
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
            cfg!(feature = "gds"),
            "{stderr}"
        );
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
    let engine = TransferEngine::new(EngineConfig {
        handler_threads: 1,
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
    #[cfg(feature = "gds")]
    {
        let gds = engine.stats()?.gds;
        assert_eq!(
            gds.available,
            std::env::var_os("ORBITTE_TEST_CUFILE").is_some()
        );
        assert_eq!((gds.submitted_batches, info.gds_mode), (0, None));
        if gds.available {
            return Ok(());
        }
    }
    #[cfg(feature = "io-uring")]
    let has_ring = engine.io_uring.is_some();
    #[cfg(not(feature = "io-uring"))]
    let has_ring = false;
    let memory = engine.register_memory(b"host".to_vec().into(), Permission::LocalOnly)?;
    for hint in [Transport::Gds, Transport::Unspec] {
        let request = [TransferRequest::new(hint, memory.base_address(), 0, 4)];
        for op in [TransferOp::Read, TransferOp::Write] {
            memory.write(0, b"host")?;
            let report = engine.submit_and_wait(op, &file, &request, Duration::from_secs(5));
            if hint == Transport::Unspec && has_ring {
                assert!(report?.is_complete());
                let mut bytes = [0; 4];
                memory.read(0, &mut bytes)?;
                assert_eq!(
                    &bytes,
                    if op == TransferOp::Read {
                        b"file"
                    } else {
                        b"host"
                    }
                );
            } else {
                assert!(matches!(report, Err(Error::Unavailable(_))));
            }
        }
    }
    assert_eq!(
        fs::read(&temp.path)?,
        if has_ring { b"host" } else { b"file" }
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
