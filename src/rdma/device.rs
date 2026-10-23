use std::ffi::CString;
use std::sync::{Arc, Mutex};

use super::{
    ConnectionId, Descriptor, RdmaConfig, RdmaStats, Resources as EndpointResources, check, events,
    keys, sys,
};
use crate::{Error, RegisteredMemory, Result};

/// The context/PD belongs to one runtime and outlives every CQ, QP and MR.
pub(super) struct Device {
    state: Mutex<State>,
    verbs: &'static sys::Verbs,
    pub(super) asynchronous: Arc<events::State>,
}

enum State {
    Open(Box<Resources>),
    Closed,
    Retained { operation: &'static str, code: i32 },
}

struct Resources {
    handles: sys::DeviceHandles,
    events: Option<events::Events>,
    usage: Arc<Mutex<RdmaStats>>,
    domain: [u8; 16],
    next_connection: u64,
    next_grant: u64,
    keys: keys::Keys,
}

impl Drop for Resources {
    fn drop(&mut self) {
        let mut usage = self
            .usage
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        usage.active_devices -= 1;
        usage.window_key_indices -= self.keys.len();
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
        let domain = ConnectionId::new_domain()?;
        usage
            .lock()
            .map_err(|_| Error::Internal("RDMA budget lock poisoned"))?
            .active_devices += 1;
        let owner = Arc::new(Self {
            state: Mutex::new(State::Open(Box::new(Resources {
                handles: sys::DeviceHandles::default(),
                events: None,
                usage,
                domain,
                next_connection: 1,
                next_grant: 1,
                keys: keys::Keys::new(config.max_window_key_indices),
            }))),
            verbs,
            asynchronous: Arc::default(),
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
                if let Some(asynchronous) = verbs.asynchronous {
                    resources.events = Some(events::Events::start(
                        &resources.handles,
                        asynchronous,
                        Arc::clone(&owner.asynchronous),
                        Arc::clone(&resources.usage),
                    )?);
                }
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
        self.asynchronous.check()?;
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

    pub(super) fn create_cq(&self, handles: &mut sys::Handles, capacity: u32) -> Result<()> {
        let state = self
            .state
            .lock()
            .map_err(|_| Error::Internal("RDMA device lock poisoned"))?;
        let State::Open(resources) = &*state else {
            return Err(Error::Unavailable("RDMA device is closed or retained"));
        };
        self.asynchronous.check()?;
        // SAFETY: the progress owner retains the real Device through CQ close.
        check("ibv_create_cq", unsafe {
            (self.verbs.create_cq)(handles, &resources.handles, capacity)
        })
    }

    pub(super) fn open_endpoint(
        &self,
        resources: &mut EndpointResources,
        config: &RdmaConfig,
        permission: u32,
        psn: u32,
        local: &mut Descriptor,
    ) -> Result<(ConnectionId, u64)> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| Error::Internal("RDMA device lock poisoned"))?;
        let State::Open(device) = &mut *state else {
            return Err(Error::Unavailable("RDMA device is closed or retained"));
        };
        self.asynchronous.check()?;
        let incarnation = ConnectionId::next(device.domain, &mut device.next_connection)?;
        let grant_generation = if permission != 0 {
            let generation = device.next_grant;
            device.next_grant = generation.checked_add(1).ok_or(Error::ResourceExhausted)?;
            generation
        } else {
            0
        };
        // SAFETY: the request and progress lane retain the Device and shared CQ;
        // its mutex excludes progress/cleanup while these handles initialize.
        let code = unsafe {
            (self.verbs.open)(
                &mut resources.handles,
                &device.handles,
                config.port,
                config.gid_index,
                resources.buffer.address().as_ptr().cast(),
                resources.buffer.len(),
                permission,
                psn,
                config.max_send_wr as u32,
                local,
            )
        };
        if !resources.handles.mw.is_null() {
            let mut usage = device.usage.lock().unwrap_or_else(|p| p.into_inner());
            usage.active_windows += 1;
            usage.opened_windows += 1;
        }
        check("ibverbs endpoint construction", code)?;
        if permission != 0 {
            if resources.handles.mw.is_null() {
                return Err(Error::Internal("source endpoint has no MW"));
            }
            let previous = device.keys.len();
            let key = device.keys.burn(resources.handles.window_key);
            let mut usage = device.usage.lock().unwrap_or_else(|p| p.into_inner());
            usage.window_key_indices += device.keys.len() - previous;
            let key = match key {
                Ok(key) => key,
                Err(error) => {
                    usage.window_key_exhaustions += 1;
                    return Err(error);
                }
            };
            usage.burned_window_keys += 1;
            resources.handles.window_key = key;
            local.rkey = key;
        }
        Ok((incarnation, grant_generation))
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
                    if operation == "ibv_close_device" {
                        if let Some(events) = &mut resources.events {
                            if let Err(Error::Unquiesced { operation, code }) = events.stop() {
                                resources
                                    .usage
                                    .lock()
                                    .unwrap_or_else(|p| p.into_inner())
                                    .retained_devices += 1;
                                let previous = std::mem::replace(
                                    &mut *state,
                                    State::Retained { operation, code },
                                );
                                std::mem::forget(previous);
                                return Err(Error::Unquiesced { operation, code });
                            }
                        }
                    }
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
