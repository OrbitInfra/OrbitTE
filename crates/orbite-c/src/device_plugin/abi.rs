//! Frozen TENT records. The separate descriptor does not change their layout.
use std::ffi::{c_char, c_int, c_void};

pub const HANDLED_ERROR: c_int = -3;
#[repr(C)]
#[derive(Clone, Copy)]
pub struct Location {
    pub start: *mut c_void,
    pub length: usize,
    pub location: [c_char; 32],
}
#[repr(C)]
#[derive(Clone, Copy, Default)]
pub struct Table {
    pub class_name: *const c_char,
    pub create_plugin: Option<unsafe extern "C" fn() -> *mut c_void>,
    pub destroy_plugin: Option<unsafe extern "C" fn(*mut c_void) -> c_int>,
    pub alloc:
        Option<unsafe extern "C" fn(*mut c_void, *mut *mut c_void, usize, *const c_char) -> c_int>,
    pub free: Option<unsafe extern "C" fn(*mut c_void, *mut c_void, usize) -> c_int>,
    pub memcpy_sync:
        Option<unsafe extern "C" fn(*mut c_void, *mut c_void, *mut c_void, usize) -> c_int>,
    pub query_location: Option<
        unsafe extern "C" fn(*mut c_void, *mut c_void, usize, *mut Location, usize) -> c_int,
    >,
    pub get_device_count: Option<unsafe extern "C" fn(*mut c_void) -> c_int>,
    pub get_device_pci_bus_id:
        Option<unsafe extern "C" fn(*mut c_void, c_int, *mut c_char, usize) -> c_int>,
}
impl Table {
    pub fn complete(&self) -> bool {
        !self.class_name.is_null()
            && self.create_plugin.is_some()
            && self.destroy_plugin.is_some()
            && self.alloc.is_some()
            && self.free.is_some()
            && self.memcpy_sync.is_some()
            && self.query_location.is_some()
            && self.get_device_count.is_some()
            && self.get_device_pci_bus_id.is_some()
    }
}
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Descriptor {
    pub magic: [u8; 8],
    pub revision: u32,
    pub pointer_bits: u32,
    pub table_size: u32,
    pub table_align: u32,
    pub location_size: u32,
    pub location_align: u32,
    pub offsets: [u32; 12],
}
pub const DESCRIPTOR: Descriptor = Descriptor {
    magic: *b"ORBIPLUG",
    revision: 1,
    pointer_bits: usize::BITS,
    table_size: size_of::<Table>() as u32,
    table_align: align_of::<Table>() as u32,
    location_size: size_of::<Location>() as u32,
    location_align: align_of::<Location>() as u32,
    offsets: [
        std::mem::offset_of!(Table, class_name) as u32,
        std::mem::offset_of!(Table, create_plugin) as u32,
        std::mem::offset_of!(Table, destroy_plugin) as u32,
        std::mem::offset_of!(Table, alloc) as u32,
        std::mem::offset_of!(Table, free) as u32,
        std::mem::offset_of!(Table, memcpy_sync) as u32,
        std::mem::offset_of!(Table, query_location) as u32,
        std::mem::offset_of!(Table, get_device_count) as u32,
        std::mem::offset_of!(Table, get_device_pci_bus_id) as u32,
        std::mem::offset_of!(Location, start) as u32,
        std::mem::offset_of!(Location, length) as u32,
        std::mem::offset_of!(Location, location) as u32,
    ],
};
