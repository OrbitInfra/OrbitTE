//! Retained Python buffer and CUDA interchange owners. No integer is an owner.
use super::{InvalidArgumentError, NotImplementedError};
use crate::memory::{ExportKind, Owner};
use pyo3::{buffer::PyBuffer, prelude::*, types::PyMemoryView};
use std::sync::Arc;

#[cfg(feature = "cuda")]
mod dlpack;

pub(super) fn get(value: &Bound<'_, PyAny>) -> PyResult<(usize, usize, Owner)> {
    let interface = match value.getattr("__cuda_array_interface__") {
        Ok(v) => Some(v),
        Err(e) if e.is_instance_of::<pyo3::exceptions::PyAttributeError>(value.py()) => None,
        Err(e) => return Err(e),
    };
    if let Some(interface) = interface {
        #[cfg(not(feature = "cuda"))]
        {
            let _ = interface;
            return Err(unavailable());
        }
        #[cfg(feature = "cuda")]
        return cuda_array(value, &interface);
    }
    if value.hasattr("__dlpack__")? && value.hasattr("__dlpack_device__")? {
        let (kind, device): (i32, i32) = value.call_method0("__dlpack_device__")?.extract()?;
        if !matches!(kind, 1 | 2) || device < 0 {
            return Err(NotImplementedError::new_err(
                "DLPack registration requires CUDA device memory; use a writable buffer for host storage",
            ));
        }
        if kind == 2 {
            #[cfg(not(feature = "cuda"))]
            return Err(unavailable());
            #[cfg(feature = "cuda")]
            return dlpack::get(value, device);
        }
        // CPU exporters such as NumPy also implement DLPack. Preserve their
        // existing buffer-protocol path, including its writable-memoryview loan.
    }
    let view = PyMemoryView::from(value)?.call_method1("cast", ("B",))?;
    let buffer = PyBuffer::<u8>::get(&view)?;
    if buffer.readonly() || !buffer.is_c_contiguous() || buffer.len_bytes() == 0 {
        return Err(invalid(
            "registration needs writable, contiguous, nonempty storage",
        ));
    }
    Ok((
        buffer.buf_ptr() as usize,
        buffer.len_bytes(),
        Owner {
            keep: Arc::new(view.unbind()),
            kind: ExportKind::Host,
        },
    ))
}
fn invalid(message: &str) -> PyErr {
    InvalidArgumentError::new_err(message.to_owned())
}
#[cfg(not(feature = "cuda"))]
fn unavailable() -> PyErr {
    NotImplementedError::new_err(
        "CUDA exporters unavailable: install an orbite wheel built with the cuda feature",
    )
}

#[cfg(feature = "cuda")]
fn extent(shape: &[i64], strides: Option<&[i64]>, item: usize) -> PyResult<usize> {
    if shape.len() > 64 || item == 0 || strides.is_some_and(|s| s.len() != shape.len()) {
        return Err(invalid("export shape/strides or item size"));
    }
    let mut bytes = item;
    for (i, &n) in shape.iter().enumerate().rev() {
        let n = usize::try_from(n).map_err(|_| invalid("negative export shape"))?;
        if n == 0
            || strides
                .is_some_and(|s| s[i] < 0 || (n > 1 && usize::try_from(s[i]).ok() != Some(bytes)))
        {
            return Err(invalid("export must be nonempty and C-contiguous"));
        }
        bytes = bytes
            .checked_mul(n)
            .filter(|n| *n <= isize::MAX as usize)
            .ok_or_else(|| invalid("export extent overflow"))?;
    }
    Ok(bytes)
}
#[cfg(feature = "cuda")]
fn vector(value: &Bound<'_, PyAny>) -> PyResult<Vec<i64>> {
    if value.len()? > 64 {
        return Err(invalid("export rank exceeds 64"));
    }
    value.extract()
}
#[cfg(feature = "cuda")]
fn range(address: usize, length: usize) -> PyResult<()> {
    if address == 0
        || length == 0
        || length > isize::MAX as usize
        || address.checked_add(length).is_none()
    {
        return Err(invalid("export pointer/extent overflow"));
    }
    Ok(())
}
#[cfg(feature = "cuda")]
fn cuda_array(
    value: &Bound<'_, PyAny>,
    interface: &Bound<'_, PyAny>,
) -> PyResult<(usize, usize, Owner)> {
    use pyo3::types::PyDict;
    let info = interface.cast::<PyDict>()?;
    let required = |key: &str| {
        info.get_item(key)?
            .ok_or_else(|| invalid("incomplete CUDA array interface"))
    };
    let version: u32 = required("version")?.extract()?;
    if !matches!(version, 2 | 3) {
        return Err(invalid("CUDA array interface needs version 2 or 3"));
    }
    let (address, readonly): (usize, bool) = required("data")?.extract()?;
    if readonly {
        return Err(invalid("CUDA exporter is read-only"));
    }
    if info.get_item("mask")?.is_some_and(|v| !v.is_none()) {
        return Err(invalid("masked CUDA arrays are unsupported"));
    }
    let shape = vector(&required("shape")?)?;
    let strides = info
        .get_item("strides")?
        .filter(|v| !v.is_none())
        .map(|v| vector(&v))
        .transpose()?;
    let typestr: String = required("typestr")?.extract()?;
    let b = typestr.as_bytes();
    if b.len() < 3 || b.len() > 32 || !b"<>=|".contains(&b[0]) || !b"biufcV?".contains(&b[1]) {
        return Err(invalid("CUDA array interface typestr"));
    }
    let item: usize = typestr
        .get(2..)
        .ok_or_else(|| invalid("CUDA dtype"))?
        .parse()
        .map_err(|_| invalid("CUDA dtype size"))?;
    let length = extent(&shape, strides.as_deref(), item)?;
    range(address, length)?;
    let stream = info
        .get_item("stream")?
        .filter(|v| !v.is_none())
        .map(|v| v.extract::<usize>())
        .transpose()?;
    if stream == Some(0) || (version == 2 && stream.is_some()) {
        return Err(invalid(
            "CUDA array interface stream requires version 3 and a nonzero stream",
        ));
    }
    // CAI itself has no device field; the driver is authoritative. If the same
    // producer also supplies DLPack's device claim, require it to agree.
    let device = if value.hasattr("__dlpack_device__")? {
        let (kind, id): (i32, i32) = value.call_method0("__dlpack_device__")?.extract()?;
        if kind != 2 || id < 0 {
            return Err(invalid("CUDA exporter device"));
        }
        Some(id)
    } else {
        None
    };
    Ok((
        address,
        length,
        Owner {
            keep: Arc::new(value.clone().unbind()),
            kind: ExportKind::Cuda {
                address,
                length,
                device,
                stream,
            },
        },
    ))
}
