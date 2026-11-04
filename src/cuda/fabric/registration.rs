use super::*;
use crate::RegisteredMemory;
use crate::memory::Allocation;

pub(crate) struct Registration {
    pub(crate) share: Option<Arc<Share>>,
    pin: Mutex<Option<Arc<HostPin>>>,
    retirement: std::sync::OnceLock<Result<()>>,
    pub(crate) local: bool,
}
impl Registration {
    pub(crate) fn retire(&self) -> Result<()> {
        self.retirement
            .get_or_init(|| {
                let pin = self.pin.lock().unwrap_or_else(|p| p.into_inner()).take();
                if let Some(pin) = pin {
                    pin.host.clone().retire_mnnvl_pin(pin)?
                }
                Ok(())
            })
            .as_ref()
            .map(|_| ())
            .map_err(Error::for_report)
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
            pin: Mutex::default(),
            retirement: Default::default(),
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
                    slot.pin = Mutex::new(Some(pin));
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
    pub(super) fn with_allocation<T>(
        &self,
        operation: impl FnOnce(u64, *const sys::CUmemAllocationProp) -> Result<T>,
    ) -> Result<T> {
        let context = &self.context;
        let result = context.current(|| {
            let mut handle = 0;
            // SAFETY: the source VMM owner retains the complete allocation and its base.
            let acquired = check("cuMemRetainAllocationHandle", unsafe {
                (self.api.retain)(&mut handle, self.address as usize as *mut c_void)
            });
            let result = acquired.and_then(|()| {
                if handle == 0 {
                    return Err(Error::Protocol("CUDA returned a null retained handle"));
                }
                let expected = location(self.loc, context.device);
                let mut storage =
                    std::mem::MaybeUninit::new(allocation_properties(self.kind, expected));
                let raw = storage.as_mut_ptr();
                // SAFETY: storage has the exact generated ABI and outlives the native call.
                check("cuMemGetAllocationPropertiesFromHandle", unsafe {
                    (self.api.properties)(raw, handle)
                })?;
                // SAFETY: CUDA initialized these C fields. requestedHandleTypes is a bit mask
                // and can contain a combination with no Rust enum discriminant. Read the raw
                // integer representation without ever forming a reference to that Rust enum.
                let (ty, location_type, id, types) = unsafe {
                    (
                        std::ptr::addr_of!((*raw).type_).cast::<u32>().read(),
                        std::ptr::addr_of!((*raw).location.type_)
                            .cast::<u32>()
                            .read(),
                        std::ptr::addr_of!((*raw).location.id).read(),
                        std::ptr::addr_of!((*raw).requestedHandleTypes)
                            .cast::<u32>()
                            .read(),
                    )
                };
                if ty != sys::CUmemAllocationType::CU_MEM_ALLOCATION_TYPE_PINNED as u32
                    || location_type != expected.type_ as u32
                    || id != expected.id
                    || types & handle_bits(native_kind(self.kind)) == 0
                {
                    return Err(Error::Unavailable("CUDA VMM allocation property mismatch"));
                }
                operation(handle, raw)
            });
            if handle != 0 {
                // SAFETY: release the one temporary reference, including a partial retain output.
                check("cuMemRelease", unsafe { (self.api.release)(handle) })
                    .map_err(|e| uncertain(e, "CUDA fabric probe release"))?;
            }
            result
        });
        if matches!(result, Err(Error::Unquiesced { .. })) {
            context.poisoned.store(true, Ordering::Release);
            std::mem::forget(context.clone());
        }
        result
    }
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
        let result = self.with_allocation(|handle, _| {
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
        });
        if matches!(result, Err(Error::Unquiesced { .. })) {
            std::mem::forget(fd.take());
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
