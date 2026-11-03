use super::{MetricsConfig, expose::Scrape};
use std::{
    io::{self, Read, Write},
    net::{TcpListener, TcpStream},
    sync::{Arc, Condvar, Mutex},
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

const REQUEST_BYTES: usize = 4096;
const IO_TIMEOUT: Duration = Duration::from_millis(500);

#[derive(Default)]
struct Stop {
    stopped: Mutex<bool>,
    changed: Condvar,
}
impl Stop {
    fn stopped(&self) -> bool {
        *self.stopped.lock().unwrap_or_else(|p| p.into_inner())
    }
    fn wait(&self, d: Duration) -> bool {
        let guard = self.stopped.lock().unwrap_or_else(|p| p.into_inner());
        let (guard, _) = self
            .changed
            .wait_timeout_while(guard, d, |stop| !*stop)
            .unwrap_or_else(|p| p.into_inner());
        *guard
    }
}

#[derive(Default)]
pub(super) struct Workers {
    stop: Arc<Stop>,
    threads: Vec<JoinHandle<()>>,
}
impl Drop for Workers {
    fn drop(&mut self) {
        *self.stop.stopped.lock().unwrap_or_else(|p| p.into_inner()) = true;
        self.stop.changed.notify_all();
        for thread in self.threads.drain(..) {
            let _ = thread.join();
        }
    }
}
impl Workers {
    pub fn http(cfg: &MetricsConfig, scrape: &Arc<Scrape>) -> io::Result<Self> {
        let listener = Arc::new(TcpListener::bind((cfg.http_host.as_str(), cfg.http_port))?);
        listener.set_nonblocking(true)?;
        let mut workers = Self::default();
        for _ in 0..cfg.http_server_threads {
            let (sock, stop, scrape) = (listener.clone(), workers.stop.clone(), scrape.clone());
            workers.threads.push(
                thread::Builder::new()
                    .name("orbitte-metrics-http".into())
                    .spawn(move || {
                        while !stop.stopped() {
                            match sock.accept() {
                                Ok((client, _)) => {
                                    let _ = serve(client, &scrape);
                                }
                                Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                                Err(e) => {
                                    if e.kind() != io::ErrorKind::WouldBlock {
                                        log::warn!("metrics accept: {e}");
                                    }
                                    if stop.wait(Duration::from_millis(10)) {
                                        break;
                                    }
                                }
                            }
                        }
                    })?,
            );
        }
        Ok(workers)
    }
    pub fn report(&mut self, interval: u32, scrape: &Arc<Scrape>) -> io::Result<()> {
        if interval != 0 {
            let (stop, scrape) = (self.stop.clone(), scrape.clone());
            self.threads.push(
                thread::Builder::new()
                    .name("orbitte-metrics-log".into())
                    .spawn(move || {
                        loop {
                            log::info!("TENT Metrics: {}", scrape.summary());
                            if stop.wait(Duration::from_secs(interval.into())) {
                                break;
                            }
                        }
                    })?,
            );
        }
        Ok(())
    }
}
fn remaining(deadline: Instant) -> io::Result<Duration> {
    deadline
        .checked_duration_since(Instant::now())
        .filter(|d| !d.is_zero())
        .ok_or_else(|| io::ErrorKind::TimedOut.into())
}
fn serve(mut stream: TcpStream, s: &Scrape) -> io::Result<()> {
    // BSD can inherit O_NONBLOCK from the acceptor. Workers serve one
    // connection at a time using the same absolute timeout on all platforms.
    stream.set_nonblocking(false)?;
    let deadline = Instant::now() + IO_TIMEOUT;
    let mut request = [0; REQUEST_BYTES];
    let mut used = 0;
    let (status, mime, body) = loop {
        if let Some(end) = request[..used].windows(4).position(|s| s == b"\r\n\r\n") {
            let line = std::str::from_utf8(&request[..end])
                .ok()
                .and_then(|v| v.lines().next());
            let mut fields = line.unwrap_or_default().split_ascii_whitespace();
            let (method, path, version) = (fields.next(), fields.next(), fields.next());
            if !matches!(version, Some("HTTP/1.1" | "HTTP/1.0")) || fields.next().is_some() {
                break ("400 Bad Request", "text/plain", "bad request".into());
            }
            if method != Some("GET") {
                break (
                    "405 Method Not Allowed",
                    "text/plain",
                    "GET required".into(),
                );
            }
            break match path {
                Some("/metrics") => ("200 OK", "text/plain; version=0.0.4", s.prometheus()),
                Some("/metrics/json") => ("200 OK", "application/json", s.json().to_string()),
                Some("/metrics/summary") => ("200 OK", "text/plain", s.summary()),
                Some("/health") => ("200 OK", "text/plain", "OK".into()),
                _ => ("404 Not Found", "text/plain", "not found".into()),
            };
        }
        if used == request.len() {
            break (
                "431 Request Header Fields Too Large",
                "text/plain",
                "header too large".into(),
            );
        }
        stream.set_read_timeout(Some(remaining(deadline)?))?;
        let n = stream.read(&mut request[used..])?;
        if n == 0 {
            return Ok(());
        }
        used += n;
    };
    let header = format!(
        "HTTP/1.1 {status}\r\nContent-Type: {mime}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    for mut data in [header.as_bytes(), body.as_bytes()] {
        while !data.is_empty() {
            stream.set_write_timeout(Some(remaining(deadline)?))?;
            let n = stream.write(data)?;
            if n == 0 {
                return Err(io::ErrorKind::WriteZero.into());
            }
            data = &data[n..];
        }
    }
    Ok(())
}
