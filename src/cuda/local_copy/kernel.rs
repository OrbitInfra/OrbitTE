use super::*;
use std::collections::HashMap;
use std::ffi::c_void;

const PTX: &str = concat!(include_str!("kernel.ptx"), "\0");
pub(super) struct Kernel {
    api: driver::KernelApi,
    context: Arc<Context>,
    module: usize,
    function: usize,
    cta_budget: usize,
    slots: Vec<Scratch>,
    closed: bool,
}
#[derive(Default)]
struct Scratch {
    device: u64,
    host: usize,
}

impl Kernel {
    pub fn install(context: &Arc<Context>, config: &LocalCopyConfig) -> Result<Self> {
        let api = context
            .driver
            .local
            .kernel
            .ok_or(Error::Unavailable("CUDA mapped-copy kernel symbols"))?;
        let mut k = Self {
            api,
            context: context.clone(),
            module: 0,
            function: 0,
            cta_budget: 0,
            slots: Vec::with_capacity(config.max_inflight),
            closed: false,
        };
        let size = config.max_descriptors * 24;
        let installed = context.current(|| {
            k.cta_budget = attribute(
                context,
                sys::CUdevice_attribute::CU_DEVICE_ATTRIBUTE_MULTIPROCESSOR_COUNT,
            )?
            .saturating_mul(4);
            let mut module = std::ptr::null_mut();
            // SAFETY: the embedded PTX is NUL terminated; the module belongs to this exact context.
            let status = unsafe { (api.load)(&mut module, PTX.as_ptr().cast()) };
            k.module = module as usize;
            if status != driver::SUCCESS {
                if !module.is_null() {
                    return Err(uncertain(Error::Backend {
                        operation: "cuModuleLoadData partial output",
                        code: status as i32,
                    }));
                }
                if matches!(
                    status,
                    driver::Status::CUDA_ERROR_INVALID_PTX
                        | driver::Status::CUDA_ERROR_UNSUPPORTED_PTX_VERSION
                        | driver::Status::CUDA_ERROR_NO_BINARY_FOR_GPU
                        | driver::Status::CUDA_ERROR_JIT_COMPILER_NOT_FOUND
                        | driver::Status::CUDA_ERROR_NOT_SUPPORTED
                ) {
                    log::debug!("local copy PTX load: {status:?}");
                    return Err(Error::Unavailable("CUDA local copy PTX"));
                }
                check("cuModuleLoadData local copy", status)?;
            }
            if module.is_null() {
                return Err(Error::Unavailable("null CUDA local copy module"));
            }
            let mut function = std::ptr::null_mut();
            // SAFETY: the live module owns this named entry and is held through all launches.
            let status =
                unsafe { (api.function)(&mut function, module, c"orbitte_batch_copy".as_ptr()) };
            if status == driver::Status::CUDA_ERROR_NOT_FOUND {
                return Err(Error::Unavailable("orbitte_batch_copy kernel"));
            }
            check("cuModuleGetFunction local copy", status)?;
            if function.is_null() {
                return Err(Error::Unavailable("null local CUDA kernel"));
            }
            k.function = function as usize;
            for _ in 0..config.max_inflight {
                k.slots.push(Scratch::default());
                let slot = k
                    .slots
                    .last_mut()
                    .ok_or(Error::Internal("missing descriptor slot"))?;
                let mut host = std::ptr::null_mut();
                // SAFETY: the bounded, nonzero slot is private DMA source storage.
                let allocated = unsafe { (context.driver.host_allocate)(&mut host, size, 1) };
                slot.host = host as usize;
                if allocated != driver::SUCCESS && !host.is_null() {
                    return Err(uncertain(Error::Backend {
                        operation: "cuMemHostAlloc partial descriptor slot",
                        code: allocated as i32,
                    }));
                }
                check("cuMemHostAlloc descriptor slot", allocated)?;
                if host.is_null() {
                    return Err(Error::Internal("null host descriptor slot"));
                }
                // SAFETY: each slot owns a separate device allocation until its event retires.
                let allocated = unsafe { (context.driver.allocate)(&mut slot.device, size) };
                if allocated != driver::SUCCESS && slot.device != 0 {
                    return Err(uncertain(Error::Backend {
                        operation: "cuMemAlloc partial descriptor slot",
                        code: allocated as i32,
                    }));
                }
                check("cuMemAlloc descriptor slot", allocated)?;
                if slot.device == 0 {
                    return Err(Error::Internal("null device descriptor slot"));
                }
            }
            Ok(())
        });
        if let Err(e) = installed {
            if matches!(e, Error::Unquiesced { .. }) {
                std::mem::forget(k);
                return Err(e);
            }
            if let Err(cleanup) = k.close() {
                std::mem::forget(k);
                return Err(cleanup);
            }
            return Err(e);
        }
        Ok(k)
    }

    pub fn descriptors(&self, plan: &plan::Plan, direction: CopyDirection) -> Result<Vec<u64>> {
        let mut aliases = HashMap::new();
        let mut descs = Vec::with_capacity(plan.rows.len() * 3);
        for row in &plan.rows {
            let base = match aliases.entry(row.registration) {
                std::collections::hash_map::Entry::Occupied(e) => *e.get(),
                std::collections::hash_map::Entry::Vacant(e) => {
                    let mut mapped = 0;
                    // SAFETY: this physical registration and every logical suballocation
                    // are retained. Resolve its GPU alias in the execution context.
                    let status = unsafe {
                        (self.api.mapped)(&mut mapped, row.pin_base as usize as *mut c_void, 0)
                    };
                    if matches!(
                        status,
                        driver::Status::CUDA_ERROR_INVALID_VALUE
                            | driver::Status::CUDA_ERROR_NOT_SUPPORTED
                    ) {
                        return Err(Error::Unavailable(
                            "pinned host registration has no device mapping",
                        ));
                    }
                    check("cuMemHostGetDevicePointer", status)?;
                    if mapped == 0 {
                        return Err(Error::Unavailable("null mapped host alias"));
                    }
                    *e.insert(mapped)
                }
            };
            let mapped = row
                .host
                .checked_sub(row.pin_base)
                .and_then(|o| base.checked_add(o))
                .ok_or(Error::InvalidRange)?;
            mapped
                .checked_add(row.width as u64)
                .ok_or(Error::InvalidRange)?;
            let (dst, src) = direction.ends(row.device, mapped);
            descs.extend([dst, src, row.width as u64]);
        }
        Ok(descs)
    }

    pub fn enqueue(
        &self,
        index: usize,
        triples: &[u64],
        max_bytes: usize,
        stream: sys::CUstream,
    ) -> Result<()> {
        let slot = &self.slots[index];
        let n = triples.len() / 3;
        let ctas = (self.cta_budget / n)
            .clamp(1, 16)
            .min(max_bytes.div_ceil(65536).clamp(1, 16));
        let grid = (n * ctas).min(65535) as u32;
        // SAFETY: the slot is exclusively reserved for this generation and sized
        // for max_descriptors triples. The pinned upload source remains live through the event.
        unsafe {
            std::ptr::copy_nonoverlapping(triples.as_ptr(), slot.host as *mut u64, triples.len());
            check(
                "cuMemcpyHtoDAsync descriptor upload",
                (self.context.driver.to_device)(
                    slot.device,
                    slot.host as *const c_void,
                    std::mem::size_of_val(triples),
                    stream,
                ),
            )?;
        }
        let mut device = slot.device;
        let mut count = n as i32;
        let mut shards = ctas as i32;
        let mut args = [
            (&mut device as *mut u64).cast(),
            (&mut count as *mut i32).cast(),
            (&mut shards as *mut i32).cast(),
        ];
        // SAFETY: the parameters match the embedded PTX entry. CUDA captures argument
        // values at launch. All device and mapped host bytes remain loaned until the event.
        check("cuLaunchKernel local copy", unsafe {
            (self.api.launch)(
                self.function as _,
                grid,
                1,
                1,
                256,
                1,
                1,
                0,
                stream,
                args.as_mut_ptr(),
                std::ptr::null_mut(),
            )
        })
    }
    pub fn close(&mut self) -> Result<()> {
        if self.closed {
            return Ok(());
        }
        self.closed = true;
        self.context
            .current(|| {
                for slot in &self.slots {
                    if slot.device != 0 {
                        // SAFETY: close runs once, after all slot receipts or before any work.
                        check("cuMemFree descriptor slot", unsafe {
                            (self.context.driver.free)(slot.device)
                        })?;
                    }
                    if slot.host != 0 {
                        // SAFETY: the matching upload has retired and this is the sole owner.
                        check("cuMemFreeHost descriptor slot", unsafe {
                            (self.context.driver.host_free)(slot.host as _)
                        })?;
                    }
                }
                if self.module != 0 {
                    // SAFETY: every launch referencing this module has a successful release receipt.
                    check("cuModuleUnload local copy", unsafe {
                        (self.api.unload)(self.module as _)
                    })?;
                }
                Ok(())
            })
            .map_err(uncertain)
    }
}
