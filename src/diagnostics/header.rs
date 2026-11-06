//! Diagnostics-only C interface. Exchanging UTF-8 documents avoids exposing
//! Rust or C++ allocator and object layouts to a native caller.
#![allow(unsafe_code)]
use super::{RedactionMode, Redactor, Sensitivity, Snapshot, version_snapshot};
use serde::{Deserialize, de::DeserializeOwned};
use serde_json::{Value, json};
use std::ffi::c_void;

type Emit = Option<extern "C" fn(*const u8, usize, *mut c_void) -> i32>;

fn render(
    snapshot: &Snapshot,
    format: u32,
    strict_warnings: bool,
    emit: Emit,
    context: *mut c_void,
) -> i32 {
    let Some(emit) = emit else { return 2 };
    let text = match format {
        0 => snapshot.render_text(),
        1 => match snapshot.render_json() {
            Ok(s) => s,
            Err(_) => return 4,
        },
        _ => return 2,
    };
    if emit(text.as_ptr(), text.len(), context) != 0 {
        4
    } else {
        snapshot.exit_code(strict_warnings).into()
    }
}

// SAFETY: uniquely named symbol with the signature in include/orbite/diagnostics.h.
#[unsafe(no_mangle)]
pub extern "C" fn orbite_diagnostics_version(format: u32, emit: Emit, context: *mut c_void) -> i32 {
    render(&version_snapshot(), format, false, emit, context)
}

unsafe fn read_json<T: DeserializeOwned>(data: *const u8, len: usize) -> Option<T> {
    if data.is_null() || len > 1 << 20 {
        return None;
    }
    // SAFETY: the native caller borrows a readable buffer of len bytes for this call.
    serde_json::from_slice(unsafe { std::slice::from_raw_parts(data, len) }).ok()
}

// SAFETY: uniquely named symbol with the signature in include/orbite/diagnostics.h.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn orbite_diagnostics_render(
    data: *const u8,
    len: usize,
    format: u32,
    strict_warnings: bool,
    emit: Emit,
    context: *mut c_void,
) -> i32 {
    // SAFETY: the header requires data to remain readable until the call returns.
    let Some(snapshot) = (unsafe { read_json::<Snapshot>(data, len) }) else {
        return 2;
    };
    if snapshot.schema_version != super::SCHEMA_VERSION {
        return 2;
    }
    render(&snapshot, format, strict_warnings, emit, context)
}

#[derive(Deserialize)]
struct Field {
    field: String,
    value: Value,
    #[serde(default)]
    sensitivity: Sensitivity,
}
#[derive(Deserialize)]
struct RedactionInput {
    mode: RedactionMode,
    fields: Vec<Field>,
    text: String,
}

// SAFETY: uniquely named symbol with the signature in include/orbite/diagnostics.h.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn orbite_diagnostics_redact(
    data: *const u8,
    len: usize,
    emit: Emit,
    context: *mut c_void,
) -> i32 {
    let Some(emit) = emit else { return 2 };
    // SAFETY: the header requires data to remain readable until the call returns.
    let Some(input) = (unsafe { read_json::<RedactionInput>(data, len) }) else {
        return 2;
    };
    let mut redactor = Redactor::new(input.mode);
    let fields: Vec<_> = input
        .fields
        .into_iter()
        .map(|f| redactor.sanitize(&f.field, f.value, f.sensitivity))
        .collect();
    let text = redactor.sanitize_text(&input.text);
    let doc = json!({"fields":fields,"text":text,"redaction":redactor.metadata()}).to_string();
    if emit(doc.as_ptr(), doc.len(), context) == 0 {
        0
    } else {
        4
    }
}
