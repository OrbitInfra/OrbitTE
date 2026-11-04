//! Native device discovery, PD creation, route queries and asynchronous events.

use super::*;

pub(in crate::transport::rdma) fn devices() -> Result<Vec<(String, String)>> {
    // This inventory operation never opens a provider context.
    // SAFETY: The provider owns count device pointers and their C names until
    // ibv_free_device_list.
    unsafe {
        let mut count = 0;
        let list = ffi::ibv_get_device_list(&mut count);
        if list.is_null() {
            return Err(Error::Backend {
                operation: "ibv_get_device_list",
                code: failure(),
            });
        }
        let mut result = Vec::new();
        for index in 0..count {
            let device = *list.add(index as usize);
            let name = ffi::ibv_get_device_name(device);
            if name.is_null() {
                continue;
            }
            let node: Vec<_> = (*device)
                .dev_name
                .iter()
                .take_while(|ch| **ch != 0)
                .map(|ch| ch.to_ne_bytes()[0])
                .collect();
            if let (Ok(name), Ok(node)) = (CStr::from_ptr(name).to_str(), String::from_utf8(node)) {
                if !node.is_empty() && !node.contains('/') {
                    result.push((name.to_owned(), node));
                }
            }
        }
        ffi::ibv_free_device_list(list);
        Ok(result)
    }
}

pub(super) unsafe fn query_descriptor(
    device: *const DeviceHandles,
    port: u8,
    gid_index: u8,
) -> std::result::Result<Descriptor, c_int> {
    // SAFETY: The caller retains the opened device throughout the synchronous descriptor query.
    let (descriptor, active, _) = unsafe { read_descriptor(device, port, gid_index) }?;
    if !active {
        return Err(libc::ENETDOWN);
    }
    Ok(descriptor)
}

unsafe fn read_descriptor(
    device: *const DeviceHandles,
    port: u8,
    gid_index: u8,
) -> std::result::Result<(Descriptor, bool, Option<u64>), c_int> {
    // SAFETY: The caller retains the device context; the port query and gid query write to
    // local outputs.
    unsafe {
        let attr = super::port::query_port((*device).context.cast(), port)?;
        let active = attr.state == ffi::ibv_port_state::IBV_PORT_ACTIVE;
        let mtu = attr.active_mtu as u8;
        if !matches!(attr.link_layer, 1 | 2)
            || !(1..=5).contains(&attr.state)
            || mtu > 5
            || (active && mtu == 0)
            || i32::from(gid_index) >= attr.gid_tbl_len
        {
            return Err(libc::EADDRNOTAVAIL);
        }
        let mut gid = ffi::ibv_gid { raw: [0; 16] };
        let code = ffi::ibv_query_gid(
            (*device).context.cast(),
            port,
            i32::from(gid_index),
            &mut gid,
        );
        if code != 0 {
            return Err(code);
        }
        if gid.raw == [0; 16] || std::net::Ipv6Addr::from(gid.raw).is_multicast() {
            return Err(libc::EADDRNOTAVAIL);
        }
        Ok((
            Descriptor {
                lid: attr.lid,
                mtu,
                link_layer: attr.link_layer,
                gid: gid.raw,
                ..Descriptor::default()
            },
            active,
            super::port::link_speed_mbps(&attr),
        ))
    }
}

pub(super) fn validate_gid_entry(entry: &ffi::ibv_gid_entry, address: &RdmaAddress) -> Result<()> {
    let (kind, link_layer) = match address.gid_type {
        discovery::GidType::Ib => (ffi::IBV_GID_TYPE_IB, 1),
        discovery::GidType::RoceV1 => (ffi::IBV_GID_TYPE_ROCE_V1, 2),
        discovery::GidType::RoceV2 => (ffi::IBV_GID_TYPE_ROCE_V2, 2),
    };
    if entry.gid_index != u32::from(address.gid_index)
        || entry.port_num != u32::from(address.port)
        // SAFETY: The queried GID is initialized and its raw arm is 16 bytes with no invalid
        // bit patterns.
        || unsafe { entry.gid.raw } != address.descriptor.gid
        || entry.gid_type != kind
        || address.descriptor.link_layer != link_layer
        || entry.ndev_ifindex != address.net_device_index
    {
        return Err(Error::Backend {
            operation: "RDMA route query changed",
            code: libc::ESTALE,
        });
    }
    Ok(())
}

unsafe fn read_address(
    device: *const DeviceHandles,
    expected: &RdmaAddress,
) -> Result<(RdmaAddress, ffi::ibv_gid_entry)> {
    #[cfg(test)]
    if let Some(result) = tests::take_address_read(device) {
        return result;
    }

    // SAFETY: The caller keeps the device and its selected port alive through both queries.
    let (descriptor, active, link_speed_mbps) = unsafe {
        read_descriptor(device, expected.port, expected.gid_index)
    }
    .map_err(|code| Error::Backend {
        operation: "ibverbs route query",
        code,
    })?;
    // SAFETY: ibv_gid_entry is a C output struct of integers and a raw GID; zero initializes
    // every field.
    let mut entry: ffi::ibv_gid_entry = unsafe { std::mem::zeroed() };
    // SAFETY: The open device is retained and entry is writable for its exact ABI size.
    let code = unsafe {
        ffi::_ibv_query_gid_ex(
            (*device).context.cast(),
            u32::from(expected.port),
            u32::from(expected.gid_index),
            &mut entry,
            0,
            std::mem::size_of::<ffi::ibv_gid_entry>(),
        )
    };
    crate::transport::rdma::check("ibv_query_gid_ex", code)?;
    let (gid_type, net_device, net_device_index) = discovery::gid_metadata(
        std::path::Path::new("/sys"),
        &expected.device,
        expected.port,
        expected.gid_index,
        descriptor.link_layer,
    )?;
    let observed = RdmaAddress {
        active,
        link_speed_mbps,
        descriptor,
        gid_type,
        net_device,
        net_device_index,
        ..expected.clone()
    };
    Ok((observed, entry))
}

pub(super) unsafe fn query_address(
    device: *const DeviceHandles,
    expected: &RdmaAddress,
    gid_index: Option<u8>,
) -> Result<RdmaAddress> {
    if let Some(gid_index) = gid_index {
        let pinned = RdmaAddress {
            gid_index,
            ..expected.clone()
        };
        // SAFETY: The caller's device handle stays live while querying the pinned GID index.
        return unsafe { query_selected_address(device, &pinned) };
    }
    let candidates = discovery::port_addresses(
        std::path::Path::new("/sys"),
        &expected.device,
        expected.port,
        None,
    );
    let mut error = Error::Unavailable("no valid GID on the selected RDMA port");
    for candidate in candidates {
        // SAFETY: Each candidate is queried synchronously against the same caller-held device.
        match unsafe { query_selected_address(device, &candidate) } {
            Ok(address) => return Ok(address),
            Err(fault) => error = fault,
        }
    }
    Err(error)
}

unsafe fn query_selected_address(
    device: *const DeviceHandles,
    expected: &RdmaAddress,
) -> Result<RdmaAddress> {
    // SAFETY: The caller retains the open device throughout the selected address query.
    let (observed, entry) = unsafe { read_address(device, expected) }?;
    validate_gid_entry(&entry, &observed)?;
    if entry.ndev_ifindex != 0 {
        let mut name = [0; libc::IF_NAMESIZE];
        // SAFETY: name provides IF_NAMESIZE writable bytes for the interface name.
        let value = unsafe { libc::if_indextoname(entry.ndev_ifindex, name.as_mut_ptr()) };
        if value.is_null() {
            return Err(Error::Backend {
                operation: "if_indextoname",
                code: failure(),
            });
        }
        // SAFETY: On success if_indextoname returns a NUL-terminated string in the still-live
        // name array.
        if unsafe { CStr::from_ptr(value) }.to_str().ok() != observed.net_device.as_deref() {
            return Err(Error::Backend {
                operation: "RDMA network device changed",
                code: libc::ESTALE,
            });
        }
    }
    Ok(observed)
}

pub(super) fn endpoint_address(
    observed: std::result::Result<Descriptor, c_int>,
    expected: &Descriptor,
) -> std::result::Result<Descriptor, c_int> {
    match observed {
        Ok(address) if address == *expected => Ok(address),
        _ => Err(libc::ESTALE),
    }
}

pub(super) unsafe extern "C" fn open_device(
    handles: *mut DeviceHandles,
    name: *const c_char,
) -> c_int {
    // SAFETY: RdmaContext owns initialized handles, including partial construction.
    unsafe {
        let h = &mut *handles;
        let mut count = 0;
        let devices = ffi::ibv_get_device_list(&mut count);
        if devices.is_null() {
            return failure();
        }
        let mut error = libc::ENODEV;
        for index in 0..count {
            let device = *devices.add(index as usize);
            let device_name = ffi::ibv_get_device_name(device);
            if !device_name.is_null() && CStr::from_ptr(name) == CStr::from_ptr(device_name) {
                #[cfg(feature = "cuda")]
                if h.request_gpu_keys {
                    h.context = super::gpu_key::open(device);
                }
                if h.context.is_null() {
                    h.context = ffi::ibv_open_device(device).cast();
                    h.request_gpu_keys = false;
                }
                error = if h.context.is_null() { failure() } else { 0 };
                break;
            }
        }
        ffi::ibv_free_device_list(devices);
        if error != 0 {
            return error;
        }
        let mut attr = ffi::ibv_device_attr::default();
        let code = ffi::ibv_query_device(h.context.cast(), &mut attr);
        if code != 0 {
            return code;
        }
        h.read_limits = ReadDepth {
            initiator: attr.max_qp_init_rd_atom.clamp(0, 255) as u8,
            responder: attr.max_qp_rd_atom.clamp(0, 255) as u8,
        };
        h.supports_atomics = attr.atomic_cap != ffi::IBV_ATOMIC_NONE;
        if !h.supports_atomics {
            log::debug!("RDMA HCA has no remote atomics; data QPs use READ/WRITE grants");
        }
        h.async_fd = (*h.context.cast::<ffi::ibv_context>()).async_fd;
        let flags = libc::fcntl(h.async_fd, libc::F_GETFL);
        if flags == -1 || libc::fcntl(h.async_fd, libc::F_SETFL, flags | libc::O_NONBLOCK) == -1 {
            return failure();
        }
        h.pd = ffi::ibv_alloc_pd(h.context.cast()).cast();
        if h.pd.is_null() {
            return failure();
        }
        #[cfg(feature = "cuda")]
        if h.request_gpu_keys && attr.device_cap_flags & ffi::IBV_DEVICE_MEM_WINDOW_TYPE_2B != 0 {
            h.gpu_keys = super::gpu_key::caps(h);
        }
        0
    }
}

pub(super) unsafe extern "C" fn get_async_event(
    context: *mut c_void,
    event: *mut AsyncEvent,
) -> c_int {
    // SAFETY: the context owner keeps the native fd/context alive and is its
    // sole consumer. The provider initializes native only on successful get.
    unsafe {
        if ffi::ibv_get_async_event(context.cast(), (*event).native.as_mut_ptr()) != 0 {
            return failure();
        }
        let native = &*(*event).native.as_ptr();
        (*event).kind = native.event_type;
        if crate::transport::rdma::events::port_event(native.event_type) {
            (*event).port = u8::try_from(native.element.port_num).unwrap_or(0);
        }
        0
    }
}

pub(super) unsafe extern "C" fn ack_async_event(_: *mut c_void, event: *mut AsyncEvent) {
    // SAFETY: each successful get is acknowledged once before touching locks;
    // no QP/CQ pointer from this event is accessed after acknowledgment.
    unsafe { ffi::ibv_ack_async_event((*event).native.as_mut_ptr()) };
}
