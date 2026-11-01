use std::ffi::CString;
use std::ptr::NonNull;
use std::sync::{Arc, Mutex};

use super::{
    ConnectionId, Descriptor, RdmaConfig, RdmaStats, Resources as EndpointResources, check,
    discovery::RdmaAddress, events, keys, sys,
};
use crate::{Error, RegisteredMemory, Result};

mod key_domain;
use key_domain::ExhaustedWindow;

/// The context/PD belongs to one runtime and outlives every CQ, QP and MR.
pub(super) struct RdmaContext {
    state: Mutex<State>,
    verbs: &'static sys::Verbs,
    pub(super) asynchronous: Arc<events::State>,
    pub(super) counters: Arc<events::Counters>,
    pub(super) monitor: Arc<super::monitor::Monitor>,
}

pub(super) enum AddressRefresh {
    Deferred,
    Paused,
    Active(RdmaAddress),
    Resumed(RdmaAddress),
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
    exhausted_windows: Vec<ExhaustedWindow>,
    address: RdmaAddress,
    gid_index: Option<u8>,
}

impl Resources {
    fn refresh_port_speed(&self, verbs: &sys::Verbs, address: &mut RdmaAddress) {
        let observed = match verbs.query_port_speed {
            Some(query) => unsafe { query(&self.handles, address.port) },
            None => Ok(None),
        };
        address.effective_speed_query_failures = self.address.effective_speed_query_failures;
        match observed {
            Ok(rate) => {
                address.effective_speed_mbps = rate.filter(|rate| *rate != 0);
                address.effective_speed_query_failing = false;
            }
            Err(_) => {
                // A transient query fault must not replace degraded LAG bandwidth
                // with the larger encoded speed of its still-active logical port.
                address.effective_speed_mbps = self.address.effective_speed_mbps;
                address.effective_speed_query_failures =
                    address.effective_speed_query_failures.saturating_add(1);
                address.effective_speed_query_failing = true;
            }
        }
        address.link_speed_mbps = address.effective_speed_mbps.or(address.link_speed_mbps);
    }
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
                exhausted_windows: Vec::new(),
                address,
                gid_index: config.gid_index,
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
                let mut observed = unsafe {
                    (verbs.query_address)(
                        &resources.handles,
                        &resources.address,
                        Some(resources.address.gid_index),
                    )
                }?;
                let mut expected = resources.address.clone();
                expected.active = observed.active;
                expected.descriptor.lid = observed.descriptor.lid;
                expected.descriptor.mtu = observed.descriptor.mtu;
                if !observed.same_route(&expected) {
                    return Err(Error::Unavailable(
                        "RDMA discovery route changed during construction",
                    ));
                }
                if !observed.active {
                    owner.asynchronous.fence_route();
                }
                resources.refresh_port_speed(verbs, &mut observed);
                resources.address = observed;
                if let Some(asynchronous) = verbs.asynchronous {
                    let context = NonNull::new(resources.handles.context)
                        .ok_or(Error::Internal("missing RDMA context"))?;
                    resources.events = Some(owner.monitor.register(
                        super::monitor::Source::Async {
                            context,
                            fd: resources.handles.async_fd,
                            port: resources.address.port,
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

    pub(super) fn refresh_address(&self, drained: bool) -> AddressRefresh {
        // The ACK thread must not wait for a creator or native destructor.
        let Ok(mut state) = self.state.try_lock() else {
            return AddressRefresh::Deferred;
        };
        let State::Open(resources) = &mut *state else {
            return AddressRefresh::Deferred;
        };
        if !self.asynchronous.recoverable() {
            return AddressRefresh::Deferred;
        }
        let generation = self.asynchronous.generation();
        let gid_index = if self.asynchronous.paused() {
            resources.gid_index
        } else {
            Some(resources.address.gid_index)
        };
        let observed = unsafe {
            (self.verbs.query_address)(&resources.handles, &resources.address, gid_index)
        };
        let Ok(mut address) = observed else {
            if !self.asynchronous.paused() {
                self.asynchronous.fence_route();
            }
            return AddressRefresh::Paused;
        };
        if !address.active
            || address.device != resources.address.device
            || address.port != resources.address.port
            || resources
                .gid_index
                .is_some_and(|pin| pin != address.gid_index)
        {
            if !self.asynchronous.paused() {
                self.asynchronous.fence_route();
            }
            return AddressRefresh::Paused;
        }
        if !address.same_route(&resources.address) && !self.asynchronous.paused() {
            self.asynchronous.fence_route();
            return AddressRefresh::Paused;
        }
        if self.asynchronous.paused() {
            if !drained || self.asynchronous.generation() != generation {
                return AddressRefresh::Paused;
            }
            resources.refresh_port_speed(self.verbs, &mut address);
            resources.address = address.clone();
            if !self.asynchronous.resume(generation) {
                return AddressRefresh::Paused;
            }
            return AddressRefresh::Resumed(address);
        }
        resources.refresh_port_speed(self.verbs, &mut address);
        resources.address = address.clone();
        AddressRefresh::Active(address)
    }

    fn verify_address(&self, resources: &Resources) -> Result<()> {
        self.asynchronous.check()?;
        let observed = unsafe {
            (self.verbs.query_address)(
                &resources.handles,
                &resources.address,
                Some(resources.address.gid_index),
            )
        };
        if !observed.is_ok_and(|address| address.same_route(&resources.address)) {
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
        let state = self
            .state
            .lock()
            .map_err(|_| Error::Internal("RDMA device lock poisoned"))?;
        let State::Open(resources) = &*state else {
            return Err(Error::Unavailable("RDMA device is closed or retained"));
        };
        self.asynchronous.check_device()?;
        // SAFETY: the MR owner retains the exact registration and actual RdmaContext.
        memory.with_rdma_context(|| {
            check("ibv_reg_mr", unsafe {
                (self.verbs.register_memory)(
                    handles,
                    &resources.handles,
                    memory.base_address() as usize as *mut std::ffi::c_void,
                    memory.len(),
                    window_bind,
                )
            })
        })
    }

    pub(super) fn register_message(
        &self,
        handles: &mut sys::MemoryHandles,
        address: NonNull<u8>,
        length: usize,
        private_pd: *mut std::ffi::c_void,
    ) -> Result<()> {
        let state = self
            .state
            .lock()
            .map_err(|_| Error::Internal("RDMA device lock poisoned"))?;
        let State::Open(resources) = &*state else {
            return Err(Error::Unavailable("RDMA device is closed or retained"));
        };
        self.asynchronous.check()?;
        let device = sys::DeviceHandles {
            context: resources.handles.context,
            pd: if private_pd.is_null() {
                resources.handles.pd
            } else {
                private_pd
            },
            async_fd: resources.handles.async_fd,
        };
        // The send grant or physical RQ owns these bytes through native teardown.
        check("ibv_reg_mr completion message", unsafe {
            (self.verbs.register_memory)(handles, &device, address.as_ptr().cast(), length, false)
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
        let private_pd = resources.grant()?.handles.remote_mr;
        let incarnation = if private_pd {
            ConnectionId::next(ConnectionId::new_domain()?, &mut device.next_connection)?
        } else {
            incarnation
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
        let registration = match &grant.buffer {
            super::grant::Buffer::Direct {
                loan: Some(loan), ..
            } => Some(loan.registration()),
            _ => None,
        };
        let mut construct = || {
            Ok(if reused {
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
            })
        };
        let code = if let Some(memory) = registration {
            memory.with_rdma_context(construct)
        } else {
            construct()
        };
        if private_pd && !connection.handles.private_pd.is_null() {
            let mut usage = device.usage.lock().unwrap_or_else(|p| p.into_inner());
            usage.active_grant_pds += 1;
            usage.opened_grant_pds += 1;
        }
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
        let code = code?;
        if code == libc::ESTALE {
            self.asynchronous.fence_route();
        }
        check("ibverbs grant/endpoint construction", code)?;
        if permission != 0 && private_pd {
            local.rkey = grant.handles.window_key;
        } else if permission != 0 {
            if grant.handles.mw.is_null() {
                return Err(Error::Internal("source endpoint has no MW"));
            }
            let key = device.burn_window_key(&mut grant.handles, config, self.verbs)?;
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
                if let Err(Error::Unquiesced { operation, code }) =
                    resources.close_exhausted_windows(self.verbs)
                {
                    resources
                        .usage
                        .lock()
                        .unwrap_or_else(|p| p.into_inner())
                        .retained_devices += 1;
                    let previous =
                        std::mem::replace(&mut *state, State::Retained { operation, code });
                    std::mem::forget(previous);
                    return Err(Error::Unquiesced { operation, code });
                }
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
