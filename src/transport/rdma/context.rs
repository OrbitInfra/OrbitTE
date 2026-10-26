use std::ffi::CString;
use std::ptr::NonNull;
use std::sync::{Arc, Mutex};

use super::{
    ConnectionId, Descriptor, RdmaConfig, RdmaStats, Resources as EndpointResources, check,
    discovery::RdmaAddress, events, keys, sys,
};
use crate::{Error, RegisteredMemory, Result};

/// The context/PD belongs to one runtime and outlives every CQ, QP and MR.
pub(super) struct RdmaContext {
    state: Mutex<State>,
    verbs: &'static sys::Verbs,
    pub(super) asynchronous: Arc<events::State>,
    pub(super) counters: Arc<events::Counters>,
    pub(super) monitor: Arc<super::monitor::Monitor>,
}

enum State {
    Open(Box<Resources>),
    Closed,
    Retained { operation: &'static str, code: i32 },
}

struct Resources {
    handles: sys::DeviceHandles,
    events: Option<super::monitor::Registration>,
    usage: Arc<Mutex<RdmaStats>>,
    domain: [u8; 16],
    next_connection: u64,
    next_grant: u64,
    keys: keys::Keys,
    address: RdmaAddress,
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

impl RdmaContext {
    pub(super) fn open_selected(
        config: &RdmaConfig,
        verbs: &'static sys::Verbs,
        usage: Arc<Mutex<RdmaStats>>,
        counters: Arc<events::Counters>,
        address: RdmaAddress,
        monitor: Arc<super::monitor::Monitor>,
    ) -> Result<Arc<Self>> {
        let name = CString::new(address.device.as_str())
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
                address,
            }))),
            verbs,
            asynchronous: Arc::default(),
            counters,
            monitor,
        });
        let initialized = {
            let mut state = owner
                .state
                .lock()
                .map_err(|_| Error::Internal("RDMA device lock poisoned"))?;
            let State::Open(resources) = &mut *state else {
                return Err(Error::Internal("RDMA device construction state"));
            };
            // SAFETY: RdmaContext retains initialized handles even on partial open.
            let code = unsafe { (verbs.open_device)(&mut resources.handles, name.as_ptr()) };
            check("ibverbs device construction", code).and_then(|()| {
                let observed =
                    unsafe { (verbs.query_address)(&resources.handles, &resources.address) }?;
                let mut expected = resources.address.clone();
                expected.descriptor.lid = observed.descriptor.lid;
                expected.descriptor.mtu = observed.descriptor.mtu;
                if observed != expected {
                    return Err(Error::Unavailable(
                        "RDMA discovery route changed during construction",
                    ));
                }
                resources.address = observed;
                if let Some(asynchronous) = verbs.asynchronous {
                    let context = NonNull::new(resources.handles.context)
                        .ok_or(Error::Internal("missing RDMA context"))?;
                    resources.events = Some(owner.monitor.register(
                        super::monitor::Source::Async {
                            context,
                            fd: resources.handles.async_fd,
                            verbs: asynchronous,
                        },
                        Arc::clone(&owner.asynchronous),
                        Arc::clone(&owner.counters),
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

    pub(super) fn address(&self) -> Result<RdmaAddress> {
        let state = self
            .state
            .lock()
            .map_err(|_| Error::Internal("RDMA device lock poisoned"))?;
        match &*state {
            State::Open(resources) => Ok(resources.address.clone()),
            _ => Err(Error::Unavailable("RDMA device is closed or retained")),
        }
    }

    fn verify_address(&self, resources: &Resources) -> Result<()> {
        self.asynchronous.check()?;
        let observed =
            unsafe { (self.verbs.query_address)(&resources.handles, &resources.address) };
        if !observed.is_ok_and(|address| address == resources.address) {
            self.asynchronous.fence_route();
        }
        self.asynchronous.check()
    }

    pub(super) fn check_address(&self) -> Result<()> {
        let result = {
            let state = self
                .state
                .lock()
                .map_err(|_| Error::Internal("RDMA device lock poisoned"))?;
            match &*state {
                State::Open(resources) => self.verify_address(resources),
                _ => Err(Error::Unavailable("RDMA device is closed or retained")),
            }
        };
        if result.is_err() {
            self.asynchronous.notify();
        }
        result
    }

    pub(super) fn connect(
        &self,
        handles: &mut sys::ConnectionHandles,
        local: &Descriptor,
        remote: &Descriptor,
        config: &RdmaConfig,
    ) -> Result<()> {
        let state = self
            .state
            .lock()
            .map_err(|_| Error::Internal("RDMA device lock poisoned"))?;
        let State::Open(resources) = &*state else {
            return Err(Error::Unavailable("RDMA device is closed or retained"));
        };
        self.verify_address(resources)?;
        check("ibv_modify_qp RTR/RTS", unsafe {
            (self.verbs.connect)(
                handles,
                local,
                remote,
                resources.address.port,
                resources.address.gid_index,
                config.traffic_class,
                config.hop_limit,
            )
        })
    }

    pub(super) fn register_memory(
        &self,
        handles: &mut sys::MemoryHandles,
        memory: &RegisteredMemory,
        window_bind: bool,
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
        // SAFETY: the MR owner retains the exact registration and actual RdmaContext.
        check("ibv_reg_mr", unsafe {
            (self.verbs.register_memory)(
                handles,
                &resources.handles,
                address.as_ptr().cast(),
                memory.len(),
                window_bind,
            )
        })
    }

    pub(super) fn register_message(
        &self,
        handles: &mut sys::MemoryHandles,
        address: NonNull<u8>,
        length: usize,
    ) -> Result<()> {
        let state = self
            .state
            .lock()
            .map_err(|_| Error::Internal("RDMA device lock poisoned"))?;
        let State::Open(resources) = &*state else {
            return Err(Error::Unavailable("RDMA device is closed or retained"));
        };
        self.asynchronous.check()?;
        // The send grant or physical RQ owns these bytes through native teardown.
        check("ibv_reg_mr completion message", unsafe {
            (self.verbs.register_memory)(
                handles,
                &resources.handles,
                address.as_ptr().cast(),
                length,
                false,
            )
        })
    }

    pub(super) fn create_completion_channel(
        &self,
        handles: &mut sys::ChannelHandles,
        verbs: sys::CqEventVerbs,
    ) -> Result<()> {
        let state = self
            .state
            .lock()
            .map_err(|_| Error::Internal("RDMA device lock poisoned"))?;
        let State::Open(resources) = &*state else {
            return Err(Error::Unavailable("RDMA device is closed or retained"));
        };
        self.asynchronous.check()?;
        check("ibv_create_comp_channel", unsafe {
            (verbs.create)(handles, &resources.handles)
        })
    }

    pub(super) fn create_cq(
        &self,
        handles: &mut sys::CqHandles,
        capacity: u32,
        channel: *mut std::ffi::c_void,
    ) -> Result<()> {
        let state = self
            .state
            .lock()
            .map_err(|_| Error::Internal("RDMA device lock poisoned"))?;
        let State::Open(resources) = &*state else {
            return Err(Error::Unavailable("RDMA device is closed or retained"));
        };
        self.asynchronous.check()?;
        // SAFETY: the progress owner retains the real RdmaContext through CQ close.
        check("ibv_create_cq", unsafe {
            (self.verbs.create_cq)(handles, &resources.handles, capacity, channel)
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
        let owner = Arc::clone(&resources.connection);
        let mut connection = owner.lock()?;
        owner.require_live()?;
        let mut state = self
            .state
            .lock()
            .map_err(|_| Error::Internal("RDMA device lock poisoned"))?;
        let State::Open(device) = &mut *state else {
            return Err(Error::Unavailable("RDMA device is closed or retained"));
        };
        self.verify_address(device)?;
        let reused = connection.incarnation.is_some();
        let incarnation = match connection.incarnation {
            Some(id) => id,
            None => ConnectionId::next(device.domain, &mut device.next_connection)?,
        };
        let generation = device.next_grant;
        device.next_grant = generation.checked_add(1).ok_or(Error::ResourceExhausted)?;
        connection.incarnation = Some(incarnation);
        let grant = resources.grant_mut()?;
        grant.connection = Some(incarnation);
        grant.generation = generation;
        let buffer = grant.buffer.address().as_ptr().cast();
        let length = grant.buffer.len();
        // The record lock excludes posting and retirement throughout construction.
        let code = if reused {
            *local = connection.local;
            let grant = resources
                .grant
                .as_mut()
                .ok_or(Error::Internal("missing reused grant"))?;
            unsafe {
                (self.verbs.prepare_grant)(
                    &mut connection.handles,
                    &mut grant.handles,
                    &device.handles,
                    buffer,
                    length,
                    permission,
                )
            }
        } else {
            let grant = resources
                .grant
                .as_mut()
                .ok_or(Error::Internal("missing new grant"))?;
            unsafe {
                (self.verbs.open)(
                    &mut connection.handles,
                    &mut grant.handles,
                    &device.handles,
                    device.address.port,
                    device.address.gid_index,
                    &device.address.descriptor,
                    buffer,
                    length,
                    permission,
                    psn,
                    config.max_send_wr as u32,
                    config.max_cqes as u32,
                    local,
                )
            }
        };
        if !reused && !connection.handles.qp.is_null() {
            device
                .usage
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .opened_connections += 1;
        }
        let grant = resources.grant_mut()?;
        if !grant.handles.mw.is_null() {
            let mut usage = device.usage.lock().unwrap_or_else(|p| p.into_inner());
            usage.active_windows += 1;
            usage.opened_windows += 1;
        }
        if code == libc::ESTALE {
            self.asynchronous.fence_route();
        }
        check("ibverbs grant/endpoint construction", code)?;
        if permission != 0 {
            if grant.handles.mw.is_null() {
                return Err(Error::Internal("source endpoint has no MW"));
            }
            let previous = device.keys.len();
            let key = device.keys.burn(grant.handles.window_key);
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
            grant.handles.window_key = key;
            local.rkey = key;
        }
        local.address = buffer as u64;
        connection.local = Descriptor {
            address: 0,
            rkey: 0,
            ..*local
        };
        Ok((incarnation, generation))
    }

    pub(super) fn domain(&self) -> Result<[u8; 16]> {
        let state = self
            .state
            .lock()
            .map_err(|_| Error::Internal("RDMA device lock poisoned"))?;
        self.asynchronous.check()?;
        match &*state {
            State::Open(resources) => Ok(resources.domain),
            _ => Err(Error::Unavailable("RDMA device is closed or retained")),
        }
    }

    /// RdmaTransport calls this only after endpoint admission is fenced and drained;
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

impl Drop for RdmaContext {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

#[cfg(test)]
#[path = "../../../tests/unit/transport/rdma/context.rs"]
pub(crate) mod tests;
