use super::*;
use plan::Dma;

pub(super) enum Command {
    Directional(Dma),
    Batch {
        dst: Vec<u64>,
        src: Vec<u64>,
        sizes: Vec<usize>,
        call: driver::BatchEntry,
    },
}

pub(super) fn commands(
    copies: &[Dma],
    dir: CopyDirection,
    batch: Option<driver::BatchEntry>,
) -> Vec<Command> {
    let mut ops = Vec::new();
    let mut tail = copies;
    while let Some((&first, rest)) = tail.split_first() {
        if let Some(call) = batch {
            let count = tail.iter().take_while(|r| r.rows == 1).count();
            if count > 1 {
                let mut dst = Vec::with_capacity(count);
                let mut src = Vec::with_capacity(count);
                let mut sizes = Vec::with_capacity(count);
                for copy in &tail[..count] {
                    let (d, s) = dir.ends(copy.first.device, copy.first.host);
                    dst.push(d);
                    src.push(s);
                    sizes.push(copy.first.width);
                }
                ops.push(Command::Batch {
                    dst,
                    src,
                    sizes,
                    call,
                });
                tail = &tail[count..];
                continue;
            }
        }
        ops.push(Command::Directional(first));
        tail = rest;
    }
    ops
}

impl Command {
    pub fn enqueue(
        &mut self,
        ctx: &Context,
        dir: CopyDirection,
        stream: sys::CUstream,
    ) -> Result<()> {
        let copy = match self {
            Self::Batch {
                dst,
                src,
                sizes,
                call,
            } => {
                // SAFETY: these independent ranges, pointer arrays and registrations
                // are validated and owned by the flight until its completion marker.
                return unsafe { driver::enqueue_batch(*call, dst, src, sizes, stream) };
            }
            Self::Directional(copy) => copy,
        };
        let r = copy.first;
        if copy.rows == 1 {
            // SAFETY: the exclusive device and pinned-host spans cover exactly width bytes.
            return check("local asynchronous CUDA copy", unsafe {
                match dir {
                    CopyDirection::HostToDevice => {
                        (ctx.driver.to_device)(r.device, r.host as _, r.width, stream)
                    }
                    CopyDirection::DeviceToHost => {
                        (ctx.driver.to_host)(r.host as _, r.device, r.width, stream)
                    }
                }
            });
        }
        let call = ctx
            .driver
            .local
            .copy_2d
            .ok_or(Error::Unavailable("cuMemcpy2DAsync_v2"))?;
        let host = sys::CUmemorytype::CU_MEMORYTYPE_HOST;
        let device = sys::CUmemorytype::CU_MEMORYTYPE_DEVICE;
        let to_device = dir == CopyDirection::HostToDevice;
        let p = sys::CUDA_MEMCPY2D {
            srcXInBytes: 0,
            srcY: 0,
            srcMemoryType: if to_device { host } else { device },
            srcHost: r.host as _,
            srcDevice: r.device,
            srcArray: std::ptr::null_mut(),
            srcPitch: if to_device {
                copy.host_pitch
            } else {
                copy.device_pitch
            },
            dstXInBytes: 0,
            dstY: 0,
            dstMemoryType: if to_device { device } else { host },
            dstHost: r.host as _,
            dstDevice: r.device,
            dstArray: std::ptr::null_mut(),
            dstPitch: if to_device {
                copy.device_pitch
            } else {
                copy.host_pitch
            },
            WidthInBytes: r.width,
            Height: copy.rows,
        };
        // SAFETY: every row was validated in the original registered views. CUDA
        // consumes the descriptor by value and the flight retains the payload ranges.
        check("cuMemcpy2DAsync_v2 local copy", unsafe { call(&p, stream) })
    }
}
