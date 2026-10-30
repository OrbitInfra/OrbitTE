//! One pending control connection. Cancellation drops its socket without replay.

use std::io;
use std::net::SocketAddr;
use std::time::Instant;

use socket2::{Domain, Protocol, Socket, Type};

use super::{ControlChannel, Stop};
use crate::engine::remaining;
use crate::{Error, Result};

pub(crate) struct Connecting<'a> {
    socket: Option<Socket>,
    deadline: Instant,
    stopping: Stop<'a>,
}

impl<'a> Connecting<'a> {
    pub(crate) fn new(endpoint: SocketAddr, deadline: Instant, stopping: Stop<'a>) -> Result<Self> {
        stopping.check()?;
        remaining(deadline)?;
        let socket = Socket::new(
            Domain::for_address(endpoint),
            Type::STREAM,
            Some(Protocol::TCP),
        )?;
        socket.set_nonblocking(true)?;
        socket.set_tcp_nodelay(true)?;
        match socket.connect(&endpoint.into()) {
            Ok(()) => (),
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => (),
            #[cfg(unix)]
            Err(error) if error.raw_os_error() == Some(libc::EINPROGRESS) => (),
            Err(error) => return Err(error.into()),
        }
        Ok(Self {
            socket: Some(socket),
            deadline,
            stopping,
        })
    }

    pub(crate) fn poll(&mut self) -> Result<Option<ControlChannel<'a>>> {
        let result = (|| {
            self.stopping.check()?;
            remaining(self.deadline)?;
            let socket = self
                .socket
                .as_ref()
                .ok_or(Error::Internal("control connection already consumed"))?;
            if let Some(error) = socket.take_error()? {
                return Err(error.into());
            }
            match socket.peer_addr() {
                Ok(_) => (),
                Err(error) if error.kind() == io::ErrorKind::NotConnected => return Ok(None),
                Err(error) => return Err(error.into()),
            }
            let socket = self
                .socket
                .take()
                .ok_or(Error::Internal("control connection already consumed"))?;
            Ok(Some(ControlChannel {
                stream: socket.into(),
                deadline: self.deadline,
                stopping: Some(self.stopping),
                nonblocking: true,
            }))
        })();
        if result.is_err() {
            drop(self.socket.take());
        }
        result
    }
}

#[cfg(test)]
#[path = "../../../tests/unit/runtime/connection.rs"]
mod tests;
