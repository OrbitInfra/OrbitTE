use std::ffi::CString;
use std::sync::{Arc, Mutex};

use super::{Endpoint, RdmaConfig, RdmaStats, check, sys};
use crate::{Error, RegisteredMemory, Result};

/// The context/PD belongs to one runtime and outlives every CQ, QP and MR.
pub(super) struct Device {
    state: Mutex<State>,
    verbs: &'static sys::Verbs,
}

enum State {
    Open(Box<Resources>),
    Closed,
    Retained { operation: &'static str, code: i32 },
}

struct Resources {
    handles: sys::DeviceHandles,
    usage: Arc<Mutex<RdmaStats>>,
}

impl Drop for Resources {
    fn drop(&mut self) {
        self.usage
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .active_devices -= 1;
    }
}

impl Device {
    pub(super) fn open(
        config: &RdmaConfig,
        verbs: &'static sys::Verbs,
        usage: Arc<Mutex<RdmaStats>>,
    ) -> Result<Arc<Self>> {
        let name = CString::new(config.device.as_str())
            .map_err(|_| Error::InvalidConfig("RDMA device contains NUL"))?;
        usage
            .lock()
            .map_err(|_| Error::Internal("RDMA budget lock poisoned"))?
            .active_devices += 1;
        let owner = Arc::new(Self {
            state: Mutex::new(State::Open(Box::new(Resources {
                handles: sys::DeviceHandles::default(),
                usage,
            }))),
            verbs,
        });
        let initialized = {
            let mut state = owner
                .state
                .lock()
                .map_err(|_| Error::Internal("RDMA device lock poisoned"))?;
            let State::Open(resources) = &mut *state else {
                return Err(Error::Internal("RDMA device construction state"));
            };
            // SAFETY: Device retains initialized handles even on partial open.
            let code = unsafe { (verbs.open_device)(&mut resources.handles, name.as_ptr()) };
            check("ibverbs device construction", code).and_then(|()| {
                resources
                    .usage
                    .lock()
                    .map_err(|_| Error::Internal("RDMA budget lock poisoned"))?
                    .opened_devices += 1;
                Ok(())
            })
        };
        if let Err(error) = initialized {
            owner.close()?;
            return Err(error);
        }
        Ok(owner)
    }

    pub(super) fn register_local(
        &self,
        handles: &mut sys::Handles,
        memory: &RegisteredMemory,
    ) -> Result<()> {
        let address = memory.host_address()?;
        let state = self
            .state
            .lock()
            .map_err(|_| Error::Internal("RDMA device lock poisoned"))?;
        let State::Open(resources) = &*state else {
            return Err(Error::Unavailable("RDMA device is closed or retained"));
        };
        // SAFETY: the MR owner retains the exact registration and actual Device.
        check("ibv_reg_mr", unsafe {
            (self.verbs.register_local)(
                handles,
                &resources.handles,
                address.as_ptr().cast(),
                memory.len(),
            )
        })
    }

    pub(super) fn open_endpoint(
        &self,
        endpoint: &mut Endpoint,
        permission: u32,
        psn: u32,
    ) -> Result<()> {
        let state = self
            .state
            .lock()
            .map_err(|_| Error::Internal("RDMA device lock poisoned"))?;
        let State::Open(device) = &*state else {
            return Err(Error::Unavailable("RDMA device is closed or retained"));
        };
        let resources = endpoint
            .resources
            .as_mut()
            .ok_or(Error::Internal("missing RDMA endpoint"))?;
        // SAFETY: this endpoint retains the actual Device, whose handles remain
        // live through endpoint destruction. Its stable bytes remain exclusive.
        let code = unsafe {
            (self.verbs.open)(
                &mut resources.handles,
                &device.handles,
                endpoint.config.port,
                endpoint.config.gid_index,
                resources.buffer.address().as_ptr().cast(),
                resources.buffer.len(),
                permission,
                psn,
                endpoint.config.max_send_wr as u32,
                &mut endpoint.local,
            )
        };
        check("ibverbs endpoint construction", code)
    }

    /// Runtime calls this only after endpoint admission is fenced and drained;
    /// Drop calls it only when the last actual endpoint owner has gone away.
    pub(super) fn close(&self) -> Result<()> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        match &mut *state {
            State::Closed => return Ok(()),
            State::Retained { operation, code } => {
                return Err(Error::Unquiesced {
                    operation,
                    code: *code,
                });
            }
            State::Open(resources) => {
                for (operation, close) in [
                    ("ibv_dealloc_pd", self.verbs.dealloc_pd),
                    ("ibv_close_device", self.verbs.close_device),
                ] {
                    // SAFETY: no endpoint can use these handles. Successful
                    // destroys clear handles; failure keeps all dependencies.
                    let code = unsafe { close(&mut resources.handles) };
                    if code != 0 {
                        resources
                            .usage
                            .lock()
                            .unwrap_or_else(|poison| poison.into_inner())
                            .retained_devices += 1;
                        let previous =
                            std::mem::replace(&mut *state, State::Retained { operation, code });
                        std::mem::forget(previous);
                        return Err(Error::Unquiesced { operation, code });
                    }
                }
            }
        }
        *state = State::Closed;
        Ok(())
    }
}

impl Drop for Device {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

#[cfg(test)]
#[path = "../../tests/unit/rdma/device.rs"]
pub(crate) mod tests;
