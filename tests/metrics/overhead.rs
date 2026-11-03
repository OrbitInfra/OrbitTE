//! Public-API control and release benchmark for CPU-only submission and real loopback TCP.
#[cfg(feature = "metrics")]
use orbit_te::EngineConfig;
use orbit_te::{Error, Permission, Result, TransferEngine, TransferOp, TransferRequest, Transport};
use std::time::{Duration, Instant};

fn engine(_enabled: bool) -> Result<TransferEngine> {
    #[cfg(feature = "metrics")]
    {
        for _ in 0..16 {
            let port = std::net::TcpListener::bind(("127.0.0.1", 0))?
                .local_addr()?
                .port();
            let config = EngineConfig {
                metrics: orbit_te::metrics::MetricsConfig {
                    enabled: _enabled,
                    http_host: "127.0.0.1".into(),
                    http_port: port,
                    report_interval_seconds: 0,
                    ..Default::default()
                },
                ..Default::default()
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
        TransferEngine::new(Default::default())
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
    let source = engine(false)?;
    let requester = engine(enabled)?;
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
    println!(
        "{}",
        serde_json::json!({"mode":mode,"empty_batch_ns":cpu,"expired_owner_ns":expired_owner,"tcp_64b_ns":tcp})
    );
    Ok(())
}
