use super::*;
use crate::{
    EngineConfig, Error, Permission, TransferEngine, TransferOp, TransferRequest, Transport,
};
use serde_json::{Value, json};
use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    time::{Duration, Instant},
};
const WAIT: Duration = crate::test_support::EVENTUAL_PROGRESS;

fn config(port: u16) -> EngineConfig {
    EngineConfig {
        metrics: MetricsConfig {
            http_port: port,
            http_host: "127.0.0.1".into(),
            report_interval_seconds: 0,
            ..Default::default()
        },
        ..Default::default()
    }
}
use crate::test_ports::guarded_port;

fn engine() -> TransferEngine {
    let (listener, _guard) = guarded_port();
    engine_on(listener)
}
fn engine_on(listener: TcpListener) -> TransferEngine {
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    let engine = TransferEngine::new(config(port)).unwrap();
    assert_eq!(engine.metrics().unwrap().http_port(), port);
    engine
}
fn get(port: u16, request: &[u8]) -> (String, String) {
    let mut s = TcpStream::connect(("127.0.0.1", port)).unwrap();
    s.set_read_timeout(Some(WAIT)).unwrap();
    s.write_all(request).unwrap();
    let mut reply = String::new();
    s.read_to_string(&mut reply).unwrap();
    let (head, body) = reply.split_once("\r\n\r\n").unwrap();
    (head.into(), body.into())
}
fn scrape(port: u16, path: &str) -> String {
    let (head, body) = get(
        port,
        format!("GET {path} HTTP/1.1\r\nHost: localhost\r\n\r\n").as_bytes(),
    );
    assert!(head.starts_with("HTTP/1.1 200"));
    body
}
mod allocation;
mod config_tests;
mod recording;
