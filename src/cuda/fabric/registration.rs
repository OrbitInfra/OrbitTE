use super::*;
use crate::RegisteredMemory;
use crate::memory::Allocation;

pub(crate) struct Registration {
    pub(crate) share: Option<Arc<Share>>,
    pin: Option<Arc<HostPin>>,
    pub(crate) local: bool,
}
impl Registration {
    pub(crate) fn retire(&mut self) -> Result<()> {
        if let Some(pin) = self.pin.take() {
            pin.host.clone().retire_mnnvl_pin(pin)?;
        }
        Ok(())
    }
}

pub(crate) struct HostPin {
    pub(crate) context: Arc<Context>,
    host: Arc<HostMemory>,
    native: bool,
}
impl HostPin {
    pub(crate) fn compatible(&self, context: &Arc<Context>, native: bool) -> bool {
        self.context.raw == context.raw && (!native || self.native)
    }
    pub(crate) fn close(&mut self) -> Result<()> {
        if !std::mem::take(&mut self.native) {
            return Ok(());
        }
        let result = self.context.current(|| {
            // SAFETY: this is the last registrar, after every retained transfer loan.
            check("cuMemHostUnregister", unsafe {
                (self.context.driver.host_unregister)(
                    self.host.base_address() as usize as *mut c_void
                )
            })
        });
        if let Err(e) = result {
            std::mem::forget((self.host.clone(), self.context.clone()));
            return Err(uncertain(e, "MNNVL host unregister"));
        }
        Ok(())
    }
}
impl Drop for HostPin {
    fn drop(&mut self) {
        if let Err(e) = self.close() {
            self.host.fence_cuda();
            log::error!("retaining MNNVL host registration: {e}")
        }
    }
}

impl Platform {
    pub(crate) fn register(&self, reg: &RegisteredMemory) -> Result<()> {
        let (owner, _) = reg.allocation_range(0, reg.len())?;
        let mut slot = Registration {
            share: None,
            pin: None,
            local: false,
        };
        match owner {
            Allocation::Cuda(m) => {
                let mut a = m
                    .allocation
                    .lock()
                    .map_err(|_| Error::Internal("CUDA allocation lock"))?;
                slot.local = m.ipc_requester;
                if let Some(v) = a.fabric.as_mut().filter(|v| v.kind == self.caps.kind) {
                    slot.share = Some(v.export()?);
                }
            }
            Allocation::Host(m) => {
                if let Some(p) = m.owned_pin() {
                    let required = p.numa_node.map(|n| self.host_device(Some(n))).transpose()?;
                    slot.local = required.is_none_or(|d| {
                        d.numa
                            == self
                                .device(p.context.device_ordinal)
                                .ok()
                                .and_then(|gpu| gpu.numa)
                    });
                    if let Some(v) = p
                        .fabric()
                        .filter(|v| self.caps.egm && v.kind == self.caps.kind)
                    {
                        slot.share = Some(v.export()?);
                    }
                } else {
                    let device = self.host_device(None)?;
                    let pin = m.mnnvl_pin(&device.context, self.config.host_register, |host| {
                        if self.config.host_register {
                            let mut entered = false;
                            let registration = device.context.current(|| {
                                entered = true;
                                // SAFETY: the HostMemory access lock covers this whole stable allocation.
                                check("cuMemHostRegister", unsafe {
                                    (device.context.driver.host_register)(
                                        host.base_address() as usize as *mut c_void,
                                        host.len(),
                                        0,
                                    )
                                })
                            });
                            if let Err(e) = registration {
                                if entered {
                                    std::mem::forget(device.context.clone());
                                    return Err(uncertain(e, "MNNVL host registration"));
                                }
                                return Err(e);
                            }
                        }
                        Ok(Arc::new(HostPin {
                            context: device.context.clone(),
                            host: host.clone(),
                            native: self.config.host_register,
                        }))
                    })?;
                    slot.pin = Some(pin);
                    slot.local = true;
                }
            }
        }
        reg.0
            .fabric
            .set(slot)
            .map_err(|_| Error::Internal("duplicate MNNVL registration"))?;
        Ok(())
    }
}
impl Mapping {
    fn export(&self) -> Result<Arc<Share>> {
        let mut cached = self
            .share
            .lock()
            .map_err(|_| Error::Internal("fabric export lock"))?;
        if let Some(share) = cached.upgrade() {
            return Ok(share);
        }
        let mut bytes = [0; 64];
        let mut fd = None;
        let context = &self.context;
        let result = context.current(|| {
            let mut handle = 0;
            // SAFETY: the owned VMM mapping is live through the probe and export.
            check("cuMemRetainAllocationHandle", unsafe {
                (self.api.retain)(&mut handle, self.address as usize as *mut c_void)
            })?;
            let export = (|| {
                let mut actual = prop(self.kind, location(self.loc, context.device), false);
                // SAFETY: the retained physical handle and typed property output are live.
                check("cuMemGetAllocationPropertiesFromHandle", unsafe {
                    (self.api.properties)(&mut actual, handle)
                })?;
                if actual.location != location(self.loc, context.device)
                    || actual.requestedHandleTypes as u32 & native_kind(self.kind) as u32 == 0
                {
                    return Err(Error::Unavailable("CUDA VMM allocation property mismatch"));
                }
                match self.kind {
                    MnnvlHandleType::Fabric => {
                        let mut fabric = sys::CUmemFabricHandle { data: [0; 64] };
                        // SAFETY: the fabric ABI writes exactly one CUmemFabricHandle.
                        check("cuMemExportToShareableHandle", unsafe {
                            (self.api.export)(
                                std::ptr::from_mut(&mut fabric).cast(),
                                handle,
                                native_kind(self.kind),
                                0,
                            )
                        })?;
                        bytes = fabric.data;
                    }
                    MnnvlHandleType::PosixFd => {
                        let mut raw = -1;
                        // SAFETY: the POSIX export ABI writes one newly owned fd.
                        let rc = unsafe {
                            (self.api.export)(
                                std::ptr::from_mut(&mut raw).cast(),
                                handle,
                                native_kind(self.kind),
                                0,
                            )
                        };
                        if raw >= 0 {
                            // SAFETY: every returned fd is closed once, including partial export failures.
                            fd = Some(unsafe { OwnedFd::from_raw_fd(raw) });
                        }
                        check("cuMemExportToShareableHandle", rc)?;
                        if raw < 0 {
                            return Err(Error::Protocol("CUDA exported invalid fd"));
                        }
                        bytes[..4].copy_from_slice(&std::process::id().to_be_bytes());
                        bytes[4..8].copy_from_slice(&raw.to_be_bytes());
                        bytes[8..24].copy_from_slice(&pid_identity(std::process::id() as i32)?);
                    }
                };
                Ok(())
            })();
            // SAFETY: release exactly the temporary reference obtained for this export.
            check("cuMemRelease", unsafe { (self.api.release)(handle) })
                .map_err(|e| uncertain(e, "CUDA fabric export release"))?;
            export
        });
        if matches!(result, Err(Error::Unquiesced { .. })) {
            context.poisoned.store(true, Ordering::Release);
            std::mem::forget((context.clone(), fd.take()));
        }
        result?;
        let share = Arc::new(Share {
            bytes,
            extent: self.size,
            location: self.loc,
            _fd: fd,
        });
        *cached = Arc::downgrade(&share);
        Ok(share)
    }
}
impl CudaMemory {
    pub(super) fn from_fabric(v: Mapping, length: usize) -> Self {
        let context = v.context.clone();
        let address = v.address;
        Self {
            address,
            length,
            location: crate::MemoryLocation::Cuda {
                device: context.device_ordinal,
                context: context.raw.as_ptr() as usize,
            },
            ipc_requester: true,
            ipc_exportable: false,
            ipc_idle: Default::default(),
            allocation: Mutex::new(DeviceAllocation {
                context,
                address,
                owned: true,
                owner: None,
                uncertain: false,
                ipc_inflight: false,
                copy_stream: None,
                producer: None,
                dma: Vec::new(),
                ipc_extent: None,
                fabric: Some(Box::new(v)),
                #[cfg(feature = "rdma")]
                rdma: None,
            }),
        }
    }
}
