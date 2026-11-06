//! Guarded release measurements for CPU submission and same-host memory transports.
use orbite::EngineConfig;
use orbite::{Error, Permission, Result, TransferEngine, TransferOp, TransferRequest, Transport};
use std::time::{Duration, Instant};

fn engine(_enabled: bool, transport: Transport) -> Result<TransferEngine> {
    let hp = transport == Transport::HpTcp;
    let base = EngineConfig {
        tcp: !hp,
        shm: orbite::ShmConfig {
            enable: transport == Transport::Shm,
            ..Default::default()
        },
        hp_tcp: orbite::HpTcpConfig {
            enable: hp,
            ..Default::default()
        },
        ..Default::default()
    };
    #[cfg(feature = "metrics")]
    {
        for _ in 0..16 {
            let port = std::net::TcpListener::bind(("127.0.0.1", 0))?
                .local_addr()?
                .port();
            let config = EngineConfig {
                metrics: orbite::metrics::MetricsConfig {
                    enabled: _enabled,
                    http_host: "127.0.0.1".into(),
                    http_port: port,
                    report_interval_seconds: 0,
                    ..Default::default()
                },
                ..base.clone()
            };
            let e = TransferEngine::new(config)?;
            if !_enabled || e.metrics().is_some_and(|m| m.http_port() != 0) {
                return Ok(e);
            }
        }
        Err(Error::Unavailable("no metrics endpoint"))
    }
    #[cfg(not(feature = "metrics"))]
    {
        TransferEngine::new(base)
    }
}
fn main() -> Result<()> {
    let mode = std::env::args().nth(1).unwrap_or_else(|| "off".into());
    let enabled = matches!(mode.as_str(), "enabled" | "serve");
    if cfg!(feature = "metrics") == (mode == "off")
        || !matches!(mode.as_str(), "off" | "disabled" | "enabled" | "serve")
    {
        return Err(Error::InvalidConfig(
            "use off without metrics or disabled/enabled/serve with metrics",
        ));
    }
    let source = engine(false, Transport::Tcp)?;
    let requester = engine(enabled, Transport::Tcp)?;
    let pattern: Vec<u8> = (0..66).map(|i| 3 * i + 7).collect();
    let remote = source.register_memory(pattern.clone().into(), Permission::RemoteReadWrite)?;
    let local = requester.register_memory(vec![0xee; 66].into(), Permission::LocalOnly)?;
    let timeout = Duration::from_secs(10);
    let peer = requester.open_segment(source.endpoint(), source.source_epoch(), timeout)?;
    let req = TransferRequest::new(
        Transport::Tcp,
        local.base_address() + 1,
        remote.base_address() + 1,
        64,
    );
    let run = |requests: &[TransferRequest]| -> Result<()> {
        let report = requester.submit_and_wait(TransferOp::Read, &peer, requests, timeout)?;
        if report.is_complete() && report.completed_requests == requests.len() {
            Ok(())
        } else {
            Err(Error::Internal("benchmark submission failed"))
        }
    };
    let verify = || -> Result<()> {
        let mut got = [0; 66];
        local.read(0, &mut got)?;
        if got[0] != 0xee || got[65] != 0xee || got[1..65] != pattern[1..65] {
            return Err(Error::Internal("TCP data or guards changed"));
        }
        remote.read(0, &mut got)?;
        if got != pattern[..] {
            return Err(Error::Internal("source bytes changed"));
        }
        Ok(())
    };
    if mode == "serve" {
        run(std::slice::from_ref(&req))?;
        verify()?;
        #[cfg(feature = "metrics")]
        println!(
            "{}",
            requester
                .metrics()
                .ok_or(Error::Internal("missing metrics"))?
                .http_port()
        );
        let mut done = String::new();
        std::io::stdin().read_line(&mut done)?;
        return Ok(());
    }
    fn measure(
        mut operation: impl FnMut() -> Result<()>,
        warm: usize,
        count: usize,
    ) -> Result<f64> {
        for _ in 0..warm {
            operation()?;
        }
        let now = Instant::now();
        for _ in 0..count {
            operation()?;
        }
        Ok(now.elapsed().as_nanos() as f64 / count as f64)
    }
    let cpu = measure(|| run(&[]), 1024, 20000)?;
    let expired = req
        .clone()
        .with_deadline(Instant::now() - Duration::from_millis(1));
    let expired_owner = measure(
        || {
            let report = requester.submit_and_wait(
                TransferOp::Read,
                &peer,
                std::slice::from_ref(&expired),
                timeout,
            )?;
            if report.completed_requests != 0
                || !matches!(report.failure.map(|f| f.error), Some(Error::Timeout))
            {
                return Err(Error::Internal("expired owner touched payload"));
            }
            Ok(())
        },
        512,
        10000,
    )?;
    let mut guard = [0; 66];
    local.read(0, &mut guard)?;
    if guard != [0xee; 66] {
        return Err(Error::Internal("expired owner modified its destination"));
    }
    let tcp = measure(|| run(std::slice::from_ref(&req)), 32, 256)?;
    verify()?;
    let roundtrip = |transport: Transport, length: usize| -> Result<(f64, f64)> {
        let source = engine(false, transport)?;
        let requester = engine(enabled, transport)?;
        let pattern: Vec<_> = (0..length + 2).map(|i| (i % 239) as u8).collect();
        let remote = if transport == Transport::Shm {
            let host = source.allocate_shm_memory(pattern.len())?;
            host.write(0, &pattern)?;
            source.register_host_memory(host, 0, pattern.len(), Permission::RemoteReadWrite)?
        } else {
            source.register_memory(pattern.clone().into(), Permission::RemoteReadWrite)?
        };
        let local =
            requester.register_memory(vec![0xee; pattern.len()].into(), Permission::LocalOnly)?;
        let peer = requester.open_segment(source.endpoint(), source.source_epoch(), timeout)?;
        let req = TransferRequest::new(
            transport,
            local.base_address() + 1,
            remote.base_address() + 1,
            length,
        );
        let run = |op| {
            let report =
                requester.submit_and_wait(op, &peer, std::slice::from_ref(&req), timeout)?;
            if report.completed_requests != 1
                || report.transferred_bytes != length
                || !report.is_complete()
            {
                return Err(Error::Internal("incomplete benchmark transfer"));
            }
            Ok(())
        };
        let (warm, repeats) = if length == 64 { (32, 256) } else { (4, 16) };
        let read = measure(|| run(TransferOp::Read), warm, repeats)?;
        let mut got = vec![0; pattern.len()];
        local.read(0, &mut got)?;
        if got[0] != 0xee || got[length + 1] != 0xee || got[1..=length] != pattern[1..=length] {
            return Err(Error::Internal("read data or guards"));
        }
        let mut written = pattern.clone();
        for byte in &mut written[1..=length] {
            *byte ^= 0x5a;
        }
        local.write(1, &written[1..=length])?;
        let write = measure(|| run(TransferOp::Write), warm, repeats)?;
        remote.read(0, &mut got)?;
        if got != written {
            return Err(Error::Internal("write data or guards"));
        }
        local.read(0, &mut got)?;
        if got[0] != 0xee || got[length + 1] != 0xee || got[1..=length] != written[1..=length] {
            return Err(Error::Internal("write source guards"));
        }
        requester.close(timeout)?;
        source.close(timeout)?;
        Ok((read, write))
    };
    let mut out = serde_json::json!({"mode": mode,"empty_batch_ns":cpu,"expired_owner_ns":expired_owner,"tcp_64b_ns":tcp});
    for (transport, label) in [
        (Transport::Tcp, "tcp"),
        (Transport::HpTcp, "hp_tcp"),
        (Transport::Shm, "shm"),
    ] {
        for (length, size) in [(64, "64b"), (4 << 20, "4mib")] {
            let (read, write) = roundtrip(transport, length)?;
            if transport != Transport::Tcp || length != 64 {
                out[format!("{label}_read_{size}_ns")] = read.into();
            }
            out[format!("{label}_write_{size}_ns")] = write.into();
        }
    }
    println!("{out}");
    Ok(())
}
