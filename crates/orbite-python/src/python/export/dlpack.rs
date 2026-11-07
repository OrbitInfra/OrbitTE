//! DLPack v1.0 C layouts (Apache-2.0, dmlc/dlpack/include/dlpack/dlpack.h).
//! A consumed capsule is renamed once; a Python-owned lease calls its deleter.
use super::*;
use pyo3::{
    exceptions::PyTypeError,
    ffi,
    types::{PyCapsule, PyDict, PyTuple},
};
use std::ffi::c_void;

#[repr(C)]
#[derive(Clone, Copy)]
struct Device {
    kind: i32,
    id: i32,
}
#[repr(C)]
#[derive(Clone, Copy)]
struct Dtype {
    code: u8,
    bits: u8,
    lanes: u16,
}
#[repr(C)]
#[derive(Clone, Copy)]
struct Tensor {
    data: *mut c_void,
    device: Device,
    ndim: i32,
    dtype: Dtype,
    shape: *const i64,
    strides: *const i64,
    offset: u64,
}
#[repr(C)]
struct Legacy {
    tensor: Tensor,
    manager: *mut c_void,
    deleter: Option<unsafe extern "C" fn(*mut Legacy)>,
}
#[repr(C)]
struct Versioned {
    major: u32,
    minor: u32,
    manager: *mut c_void,
    deleter: Option<unsafe extern "C" fn(*mut Versioned)>,
    flags: u64,
    tensor: Tensor,
}
// An integer is only used here after a validated, named producer capsule. The
// native tensor and its shape remain live until this single consumer releases it.
enum Lease {
    Legacy(usize, Option<unsafe extern "C" fn(*mut Legacy)>),
    Versioned(usize, Option<unsafe extern "C" fn(*mut Versioned)>),
}
impl Drop for Lease {
    fn drop(&mut self) {
        // SAFETY: capsule consumption transfers exactly one producer-provided
        // deleter. This lease lives in a PyCapsule, so its destruction runs with
        // the GIL, outside native locks; PyO3 defers unattached decrefs.
        unsafe {
            match *self {
                Self::Legacy(p, Some(f)) => f(p as *mut Legacy),
                Self::Versioned(p, Some(f)) => f(p as *mut Versioned),
                _ => (),
            }
        }
    }
}

pub(super) fn get(value: &Bound<'_, PyAny>, device: i32) -> PyResult<(usize, usize, Owner)> {
    let py = value.py();
    let kwargs = PyDict::new(py);
    kwargs.set_item("stream", 1)?; // CUDA legacy default stream of the exported device.
    kwargs.set_item("max_version", (1, 0))?;
    let capsule = match value.call_method("__dlpack__", (), Some(&kwargs)) {
        Ok(v) => v,
        Err(e)
            if e.is_instance_of::<PyTypeError>(py)
                && e.to_string().contains("max_version")
                && e.to_string().contains("keyword") =>
        {
            kwargs.del_item("max_version")?;
            value.call_method("__dlpack__", (), Some(&kwargs))?
        }
        Err(e) => return Err(e),
    };
    let capsule = capsule.cast::<PyCapsule>()?;
    // SAFETY: Python owns this capsule under the GIL. GetPointer checks the exact
    // protocol name before any C layout is read. Producer capsules, like any
    // extension ABI, must contain valid live managed-tensor/shape storage.
    let (tensor, lease, used) = unsafe {
        if ffi::PyCapsule_IsValid(capsule.as_ptr(), c"dltensor".as_ptr()) != 0 {
            let p =
                ffi::PyCapsule_GetPointer(capsule.as_ptr(), c"dltensor".as_ptr()).cast::<Legacy>();
            if p.is_null() {
                return Err(PyErr::fetch(py));
            }
            (
                (*p).tensor,
                Lease::Legacy(p as usize, (*p).deleter),
                c"used_dltensor",
            )
        } else if ffi::PyCapsule_IsValid(capsule.as_ptr(), c"dltensor_versioned".as_ptr()) != 0 {
            let p = ffi::PyCapsule_GetPointer(capsule.as_ptr(), c"dltensor_versioned".as_ptr())
                .cast::<Versioned>();
            if p.is_null() {
                return Err(PyErr::fetch(py));
            }
            if (*p).major != 1 || (*p).minor != 0 {
                return Err(invalid("unsupported DLPack version"));
            }
            if (*p).flags != 0 {
                return Err(invalid(
                    "DLPack export must be writable, zero-copy and have no unsupported flags",
                ));
            }
            (
                (*p).tensor,
                Lease::Versioned(p as usize, (*p).deleter),
                c"used_dltensor_versioned",
            )
        } else {
            return Err(invalid("DLPack capsule is invalid or already consumed"));
        }
    };
    // Until rename succeeds the original capsule owns the deleter. ManuallyDrop
    // prevents a second destroy on any validation or name-change failure.
    let mut lease = std::mem::ManuallyDrop::new(lease);
    if tensor.data.is_null() {
        return Err(invalid("DLPack data pointer must not be null"));
    }
    if tensor.device.kind != 2
        || tensor.device.id != device
        || !(0..=64).contains(&tensor.ndim)
        || (tensor.ndim != 0 && tensor.shape.is_null())
    {
        return Err(invalid("DLPack device or rank mismatch"));
    }
    if !matches!(tensor.dtype.code, 0..=2 | 4..=6)
        || tensor.dtype.lanes == 0
        || tensor.dtype.bits == 0
    {
        return Err(invalid("unsupported DLPack dtype"));
    }
    let bits = usize::from(tensor.dtype.bits) * usize::from(tensor.dtype.lanes);
    if bits % 8 != 0 {
        return Err(invalid("sub-byte DLPack dtype is unsupported"));
    }
    let item = bits / 8;
    // SAFETY: a valid producer tensor supplies ndim elements, bounded above by 64.
    let shape = unsafe {
        if tensor.ndim == 0 {
            &[][..]
        } else {
            std::slice::from_raw_parts(tensor.shape, tensor.ndim as usize)
        }
    };
    let strides = if tensor.strides.is_null() {
        None
    } else {
        // SAFETY: same producer contract and bounded ndim as shape. Convert the
        // DLPack element strides to bytes with checked arithmetic.
        let s = unsafe { std::slice::from_raw_parts(tensor.strides, tensor.ndim as usize) };
        Some(
            s.iter()
                .map(|n| {
                    n.checked_mul(item as i64)
                        .ok_or_else(|| invalid("DLPack stride overflow"))
                })
                .collect::<PyResult<Vec<_>>>()?,
        )
    };
    let length = extent(shape, strides.as_deref(), item)?;
    let address = (tensor.data as usize)
        .checked_add(usize::try_from(tensor.offset).map_err(|_| invalid("DLPack offset overflow"))?)
        .ok_or_else(|| invalid("DLPack pointer overflow"))?;
    range(address, length)?;
    // SAFETY: names are static C strings and GIL serialization makes one capsule
    // consumable exactly once, including a producer that returns it repeatedly.
    if unsafe { ffi::PyCapsule_SetName(capsule.as_ptr(), used.as_ptr()) } != 0 {
        return Err(PyErr::fetch(py));
    }
    // SAFETY: rename succeeded, so the original capsule no longer owns deletion.
    let lease = unsafe { std::mem::ManuallyDrop::take(&mut lease) };
    let managed = PyCapsule::new(py, lease, None)?;
    let retained = PyTuple::new(
        py,
        [
            value.clone(),
            capsule.clone().into_any(),
            managed.into_any(),
        ],
    )?;
    Ok((
        address,
        length,
        Owner {
            keep: Arc::new(retained.unbind()),
            kind: ExportKind::Cuda {
                address,
                length,
                device: Some(device),
                stream: Some(1),
            },
        },
    ))
}

#[cfg(test)]
#[path = "../../../tests/unit/python_dlpack.rs"]
mod tests;
