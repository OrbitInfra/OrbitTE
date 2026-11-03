use super::*;
use crate::metrics::schema::{self, Metric as M};
#[test]
fn concurrent_cells_keep_exact_transport_counts_and_inclusive_buckets() {
    let cells = Arc::new(Recorder::new());
    std::thread::scope(|scope| {
        for t in [
            Transport::Rdma,
            Transport::Tcp,
            Transport::Nvlink,
            Transport::IoUring,
            Transport::Gds,
        ] {
            for _ in 0..3 {
                let c = cells.clone();
                scope.spawn(move || {
                    for _ in 0..100 {
                        let obs = Observation::new(
                            &c,
                            TransferOp::Read,
                            t,
                            1024,
                            None,
                            Some(Instant::now()),
                        );
                        obs.dispatched();
                        obs.begin(t);
                        obs.finish(None, true);
                        obs.terminal(None);
                        obs.terminal(None);
                    }
                });
            }
        }
    });
    let scrape = Scrape {
        recorder: cells.clone(),
        owners: Box::new(Owners::default),
    };
    let j = scrape.json();
    assert_eq!(j["tent_read_bytes_total"], 1024 * 1500);
    assert_eq!(j["tent_read_size_bytes"]["count"], 1500);
    assert_eq!(j["tent_read_size_bytes"]["buckets"]["1024"], 1500);
    assert_eq!(j["tent_transport_attempt_latency_us"]["count"], 1500);
    for x in [0, 99, 100, 101, 1000001] {
        cells.observe(M::ReadLatency, schema::slot(Transport::Unspec), x);
    }
    cells.observe(M::QueueWait, 0, 0);
    let text = scrape.prometheus();
    assert!(
        text.contains("tent_read_latency_us_bucket{transport=\"unspec\",le=\"100.000000\"} 3\n")
    );
    assert!(text.contains("tent_read_latency_us_bucket{transport=\"unspec\",le=\"+Inf\"} 5\n"));
    assert!(text.contains("tent_stage_queue_wait_us_count{transport=\"unspec\"} 1\n"));
    assert!(text.contains("tent_stage_queue_wait_us_sum{transport=\"unspec\"} 0\n"));
    let j = scrape.json();
    for (m, bounds) in [
        ("tent_read_latency_us", schema::LATENCY),
        ("tent_write_size_bytes", schema::SIZE),
        ("tent_deadline_mlu_permille", schema::MLU),
        ("tent_stage_dispatch_us", schema::STAGE),
    ] {
        assert_eq!(j[m]["buckets"].as_object().unwrap().len(), bounds.len());
        for n in bounds {
            assert!(j[m]["buckets"].get(n.to_string()).is_some());
        }
    }
}
#[test]
fn gauges_survive_toggles_failures_keep_the_first_stage_and_quarantine_counts_once() {
    let cells = Arc::new(Recorder::new());
    let scrape = Scrape {
        recorder: cells.clone(),
        owners: Box::new(Owners::default),
    };
    let t = Transport::Rdma;
    let obs = Observation::new(
        &cells,
        TransferOp::Write,
        t,
        50,
        Some(Instant::now() - Duration::from_secs(1)),
        Some(Instant::now()),
    );
    obs.dispatched();
    obs.begin(t);
    assert_eq!(scrape.json()["tent_inflight_attempts"], 1);
    cells.enabled.store(false, Relaxed);
    obs.finish(Some(&Error::Unavailable("fault")), true);
    assert_eq!(scrape.json()["tent_inflight_attempts"], 0);
    obs.retry(Transport::Tcp);
    obs.begin(Transport::Tcp);
    assert_eq!(scrape.json()["tent_inflight_attempts"], 1);
    cells.enabled.store(true, Relaxed);
    obs.finish(Some(&Error::Unavailable("fault")), false);
    obs.terminal(Some(&Error::Unavailable("fault")));
    obs.terminal(None);
    assert_eq!(
        scrape.json()["tent_transport_attempt_latency_us"]["count"],
        0
    );
    let resumed = Observation::new(&cells, TransferOp::Read, Transport::Tcp, 1, None, None);
    resumed.dispatched();
    resumed.begin(Transport::Tcp);
    resumed.finish(None, true);
    resumed.terminal(None);
    let json = scrape.json();
    assert_eq!(json["tent_read_bytes_total"], 1);
    assert_eq!(json["tent_read_latency_us"]["count"], 0);
    assert_eq!(json["tent_stage_queue_wait_us"]["count"], 0);
    assert_eq!(json["tent_transport_attempt_latency_us"]["count"], 1);
    cells.quarantine();
    assert_eq!(scrape.json()["tent_quarantined_batches_total"], 1);
    assert_eq!(scrape.json()["tent_deadline_infeasible_total"], 1);
    assert_eq!(scrape.json()["tent_write_bytes_total"], 0);
    assert_eq!(scrape.json()["tent_write_failures_total"], 1);
    assert!(
        scrape
            .prometheus()
            .contains("tent_task_failures_total{transport=\"tcp\",reason=\"poll\"} 1")
    );
}
#[test]
fn real_tcp_merged_owner_exposes_one_transfer_and_scrapes_both_formats() {
    let source = engine();
    let requester = engine();
    let metrics = requester.metrics().unwrap();
    let port = metrics.http_port();
    let remote = source
        .register_memory(vec![0, 1, 2, 3, 4, 5].into(), Permission::RemoteReadWrite)
        .unwrap();
    let local = requester
        .register_memory(vec![0xee; 6].into(), Permission::LocalOnly)
        .unwrap();
    let peer = requester
        .open_segment(source.endpoint(), source.source_epoch(), WAIT)
        .unwrap();
    let request = |offset| {
        TransferRequest::new(
            Transport::Tcp,
            local.base_address() + offset,
            remote.base_address() + offset,
            2,
        )
        .with_deadline(Instant::now() + WAIT)
    };
    for op in [TransferOp::Read, TransferOp::Write] {
        // Both requests must carry the same absolute deadline to merge.
        let deadline = Instant::now() + WAIT;
        let r = [
            request(1).with_deadline(deadline),
            request(3).with_deadline(deadline),
        ];
        assert!(
            requester
                .submit_and_wait(op, &peer, &r, WAIT)
                .unwrap()
                .is_complete()
        );
    }
    let mut actual = [0; 6];
    local.read(0, &mut actual).unwrap();
    assert_eq!(actual, [0xee, 1, 2, 3, 4, 0xee]);
    let j: Value = serde_json::from_str(&scrape(port, "/metrics/json")).unwrap();
    for name in ["tent_read_bytes_total", "tent_write_bytes_total"] {
        assert_eq!(j[name], 4);
    }
    assert_eq!(j["tent_read_requests_total"], 1);
    assert_eq!(j["tent_transport_attempts_total"], 2);
    for name in [
        "tent_deadline_mlu_permille",
        "tent_stage_queue_wait_us",
        "tent_stage_dispatch_us",
        "tent_stage_transport_us",
    ] {
        assert_eq!(j[name]["count"], 2, "{name}");
    }
    let p = scrape(port, "/metrics");
    assert!(p.contains("tent_read_bytes_total{transport=\"tcp\"} 4"));
    assert!(p.contains("tent_registered_buffer_bytes{transport=\"tcp\"} 6"));
    metrics.set_enabled(false);
    requester.unregister_memory(&local).unwrap();
    metrics.set_enabled(true);
    assert!(
        metrics
            .prometheus()
            .contains("tent_registered_buffer_bytes{transport=\"tcp\"} 0\n")
    );
    assert_eq!(scrape(port, "/health"), "OK");
    assert!(scrape(port, "/metrics/summary").contains("Failovers: 0"));
}
#[test]
fn busy_port_degrades_to_log_only_and_drop_joins_bounded_http_workers() {
    let held = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let port = held.local_addr().unwrap().port();
    let e = TransferEngine::new(config(port)).unwrap();
    assert_eq!(e.metrics().unwrap().http_port(), 0);
    drop(e);
    drop(held);
    let engine = TransferEngine::new(config(port)).unwrap();
    assert_eq!(engine.metrics().unwrap().http_port(), port);
    assert!(
        get(port, b"POST /metrics HTTP/1.1\r\n\r\n")
            .0
            .contains("405")
    );
    assert!(
        get(port, b"GET /unknown HTTP/1.0\r\n\r\n")
            .0
            .contains("404")
    );
    assert!(get(port, &[b'x'; 4096]).0.contains("431"));
    let mut slow = TcpStream::connect(("127.0.0.1", port)).unwrap();
    slow.write_all(b"GET /").unwrap();
    drop(engine);
    let rebound = TcpListener::bind(("127.0.0.1", port)).unwrap();
    drop(rebound);
}

#[test]
fn an_expired_owner_records_a_reason_without_fabricating_bytes_or_latency() {
    let source = engine();
    let requester = engine();
    let r = source
        .register_memory(vec![1, 2, 3, 4].into(), Permission::RemoteReadWrite)
        .unwrap();
    let m = requester
        .register_memory(vec![9; 4].into(), Permission::LocalOnly)
        .unwrap();
    let peer = requester
        .open_segment(source.endpoint(), source.source_epoch(), WAIT)
        .unwrap();
    let req = TransferRequest::new(Transport::Tcp, m.base_address(), r.base_address(), 4)
        .with_deadline(Instant::now() - Duration::from_secs(1));
    let result = requester
        .submit_and_wait(TransferOp::Write, &peer, &[req], WAIT)
        .unwrap();
    assert!(matches!(result.failure.unwrap().error, Error::Timeout));
    let metrics = requester.metrics().unwrap();
    let j = metrics.json();
    assert_eq!(j["tent_deadline_infeasible_total"], 1);
    assert_eq!(j["tent_write_bytes_total"], 0);
    assert_eq!(j["tent_write_requests_total"], 0);
    assert!(
        metrics
            .prometheus()
            .contains("tent_task_failures_total{transport=\"tcp\",reason=\"timeout\"} 1")
    );
    let mut bytes = [0; 4];
    r.read(0, &mut bytes).unwrap();
    assert_eq!(bytes, [1, 2, 3, 4]);
}

#[test]
fn stalled_client_leaves_the_single_http_worker_and_long_interval_reporter_joinable() {
    let port = TcpListener::bind(("127.0.0.1", 0))
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let mut cfg = config(port);
    cfg.metrics.http_server_threads = 1;
    cfg.metrics.report_interval_seconds = 3600;
    let engine = TransferEngine::new(cfg).unwrap();
    assert_eq!(engine.metrics().unwrap().http_port(), port);
    let mut incomplete = TcpStream::connect(("127.0.0.1", port)).unwrap();
    incomplete.write_all(b"GET /health HTTP/1.1\r\n").unwrap();
    assert_eq!(scrape(port, "/health"), "OK");
    incomplete.set_read_timeout(Some(WAIT)).unwrap();
    assert_eq!(incomplete.read(&mut [0; 1]).unwrap(), 0);
    drop(engine);
    assert!(TcpListener::bind(("127.0.0.1", port)).is_ok());
}

#[test]
fn a_retired_initial_route_and_a_rejected_retry_preserve_real_attempt_counts() {
    let c = Arc::new(Recorder::new());
    let o = Observation::new(
        &c,
        TransferOp::Read,
        Transport::Gds,
        8,
        None,
        Some(Instant::now()),
    );
    o.dispatched();
    o.begin(Transport::IoUring);
    o.finish(Some(&Error::Unavailable("before posting")), false);
    o.retry(Transport::Tcp);
    o.begin(Transport::Tcp);
    o.finish(Some(&Error::Unavailable("after posting")), true);
    #[cfg(any(feature = "io-uring", feature = "gds"))]
    o.file_terminal(None);
    #[cfg(not(any(feature = "io-uring", feature = "gds")))]
    o.terminal(Some(&Error::Unavailable("after posting")));
    let s = Scrape {
        recorder: c,
        owners: Box::new(Owners::default),
    };
    let j = s.json();
    let p = s.prometheus();
    assert_eq!(j["tent_transport_attempts_total"], 2);
    assert_eq!(j["tent_transport_attempt_failures_total"], 2);
    assert_eq!(j["tent_read_failures_total"], 1);
    assert_eq!(j["tent_read_bytes_total"], 0);
    assert!(p.contains("tent_transport_failover_total{from=\"gds\",to=\"io_uring\"} 1\n"));
    assert!(p.contains("tent_task_failures_total{transport=\"tcp\",reason=\"submit\"} 1\n"));
    assert!(!p.contains("tent_transport_attempts_total{transport=\"gds\""));
}
