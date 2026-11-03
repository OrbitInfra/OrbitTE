use crate::{Error, Result};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

pub(super) const STEP: usize = 1 << 20;
pub(super) struct Budget {
    limits: (u64, u64),
    used: Mutex<(u64, u64)>,
}
impl Budget {
    pub(super) fn new(tasks: u64, bytes: u64) -> Arc<Self> {
        Arc::new(Self {
            limits: (tasks, bytes),
            used: Mutex::default(),
        })
    }
    pub(super) fn reserve(self: &Arc<Self>, bytes: usize) -> Result<Permit> {
        let bytes = u64::try_from(bytes).map_err(|_| Error::ResourceExhausted)?;
        let mut used = self
            .used
            .lock()
            .map_err(|_| Error::Internal("HP-TCP admission lock"))?;
        if used.0 >= self.limits.0 || bytes > self.limits.1 - used.1 {
            return Err(Error::ResourceExhausted);
        }
        used.0 += 1;
        used.1 += bytes;
        Ok(Permit {
            budget: self.clone(),
            bytes,
        })
    }
}
pub(crate) struct Permit {
    budget: Arc<Budget>,
    bytes: u64,
}
impl Drop for Permit {
    fn drop(&mut self) {
        let mut used = self.budget.used.lock().unwrap_or_else(|e| e.into_inner());
        used.0 -= 1;
        used.1 -= self.bytes;
    }
}

pub(super) async fn read(s: &mut TcpStream, mut buf: &mut [u8], timeout: Duration) -> Result<()> {
    while !buf.is_empty() {
        let n = tokio::time::timeout(timeout, s.read(buf))
            .await
            .map_err(|_| Error::Timeout)??;
        if n == 0 {
            return Err(std::io::Error::from(std::io::ErrorKind::UnexpectedEof).into());
        }
        buf = &mut buf[n..];
    }
    Ok(())
}
pub(super) async fn write(
    s: &mut TcpStream,
    mut buf: &[u8],
    timeout: Duration,
    posted: &mut bool,
) -> Result<()> {
    while !buf.is_empty() {
        let n = tokio::time::timeout(timeout, s.write(buf))
            .await
            .map_err(|_| Error::Timeout)??;
        if n == 0 {
            return Err(std::io::Error::from(std::io::ErrorKind::WriteZero).into());
        }
        *posted = true;
        buf = &buf[n..];
    }
    Ok(())
}
