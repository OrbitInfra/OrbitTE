use std::collections::BTreeMap;
use std::error::Error;
use std::sync::Arc;
use std::time::Duration;

use orbit_te::{
    EngineConfig, HostMemory, Notification, NotificationStatus, Permission, RegisteredMemory,
    TransferEngine, TransferOp, TransferRequest,
};

const TIMEOUT: Duration = Duration::from_secs(5);

struct Buffer {
    memory: Arc<HostMemory>,
    registration: Option<RegisteredMemory>,
    references: usize,
}

fn text(value: &str) -> String {
    if value == "-" {
        String::new()
    } else {
        value.into()
    }
}

fn hex(bytes: &[u8]) -> String {
    if bytes.is_empty() {
        return "-".into();
    }
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut result = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        result.push(char::from(DIGITS[(byte >> 4) as usize]));
        result.push(char::from(DIGITS[(byte & 15) as usize]));
    }
    result
}

fn main() -> Result<(), Box<dyn Error>> {
    let path = std::env::args().nth(1).ok_or("expected trace path")?;
    let trace = std::fs::read_to_string(path)?;
    let source = TransferEngine::new(EngineConfig::default())?;
    let requester = TransferEngine::new(EngineConfig::default())?;
    let mut buffers = BTreeMap::<String, Buffer>::new();
    let mut peer = None;
    for (line_index, line) in trace.lines().enumerate() {
        let fields: Vec<_> = line.split_whitespace().collect();
        if fields.is_empty() || fields[0].starts_with('#') {
            continue;
        }
        match fields.as_slice() {
            ["ORBITTE_CPU_TRACE", "1"] => println!("OBS\tVERSION\t1"),
            ["BUFFER", name, length, initial] => {
                if !matches!(*name, "source" | "local") || buffers.contains_key(*name) {
                    return Err("unexpected or duplicate buffer".into());
                }
                let length = length.parse::<usize>()?;
                if length == 0 || length > 1 << 20 {
                    return Err("invalid buffer length".into());
                }
                let bytes: Vec<_> = (0..length)
                    .map(|index| {
                        if *initial == "pattern" {
                            Ok((index % 251) as u8)
                        } else {
                            initial.parse::<u8>()
                        }
                    })
                    .collect::<Result<_, _>>()?;
                buffers.insert(
                    (*name).into(),
                    Buffer {
                        memory: Arc::new(HostMemory::new(bytes.into_boxed_slice())?),
                        registration: None,
                        references: 0,
                    },
                );
            }
            ["REGISTER", name, permission] => {
                let buffer = buffers.get_mut(*name).ok_or("unknown buffer")?;
                let engine = if *name == "source" {
                    &source
                } else {
                    &requester
                };
                let permission = match *permission {
                    "rw" => Permission::RemoteReadWrite,
                    "r" => Permission::RemoteRead,
                    "local" => Permission::LocalOnly,
                    _ => return Err("unknown permission".into()),
                };
                let registered = engine.register_host_memory(
                    Arc::clone(&buffer.memory),
                    0,
                    buffer.memory.len(),
                    permission,
                )?;
                if let Some(previous) = &buffer.registration {
                    if previous.id() != registered.id() {
                        return Err("duplicate registration changed identity".into());
                    }
                }
                buffer.registration = Some(registered);
                buffer.references += 1;
            }
            ["REGISTER_BATCH", name, "r", count] => {
                let count = count.parse::<usize>()?;
                if count == 0 || count > 1024 {
                    return Err("invalid registration batch size".into());
                }
                let buffer = buffers.get_mut(*name).ok_or("unknown buffer")?;
                let engine = if *name == "source" {
                    &source
                } else {
                    &requester
                };
                let previous = buffer
                    .registration
                    .as_ref()
                    .ok_or("register before batch duplicates")?;
                let ranges = vec![(Arc::clone(&buffer.memory), 0, buffer.memory.len()); count];
                let registered =
                    engine.register_host_memory_batch(&ranges, Permission::RemoteRead)?;
                if registered.len() != count
                    || registered.iter().any(|memory| memory.id() != previous.id())
                {
                    return Err("batch duplicates changed identity".into());
                }
                buffer.references += count;
                println!("OBS\tREGISTER_BATCH\t{name}\t{count}");
            }
            ["UNREGISTER", name] => {
                let buffer = buffers.get_mut(*name).ok_or("unknown buffer")?;
                let engine = if *name == "source" {
                    &source
                } else {
                    &requester
                };
                engine.unregister_memory(
                    buffer
                        .registration
                        .as_ref()
                        .ok_or("buffer not registered")?,
                )?;
                buffer.references -= 1;
                if buffer.references == 0 {
                    buffer.registration = None;
                }
            }
            ["DESCRIBE", label] => {
                let refreshed =
                    requester.open_segment(source.endpoint(), source.source_epoch(), TIMEOUT)?;
                for region in refreshed.regions() {
                    let buffer = &buffers["source"];
                    if region.base_address() != buffer.memory.base_address()
                        || region.len() != buffer.memory.len()
                    {
                        return Err("descriptor does not identify actual backing".into());
                    }
                }
                println!(
                    "OBS\tDESCRIBE\t{label}\t{}\t{}",
                    refreshed.regions().len(),
                    refreshed
                        .regions()
                        .iter()
                        .map(|region| region.len())
                        .sum::<usize>()
                );
                peer = Some(refreshed);
            }
            ["FILL", name, offset, length, value] => {
                buffers
                    .get(*name)
                    .ok_or("unknown buffer")?
                    .memory
                    .write(offset.parse()?, &vec![value.parse()?; length.parse()?])?;
            }
            ["BYTES", label, name, offset, length] => {
                let mut bytes = vec![0; length.parse()?];
                buffers
                    .get(*name)
                    .ok_or("unknown buffer")?
                    .memory
                    .read(offset.parse()?, &mut bytes)?;
                println!("OBS\tBYTES\t{label}\t{}", hex(&bytes));
            }
            [operation @ ("READ" | "WRITE"), label, slices, name, message] => {
                let peer = peer.as_ref().ok_or("DESCRIBE must precede transfers")?;
                let mut requests = Vec::new();
                if *slices != "-" {
                    for slice in slices.split(',') {
                        let parts: Vec<_> = slice
                            .split(':')
                            .map(str::parse::<usize>)
                            .collect::<Result<_, _>>()?;
                        let [local_offset, remote_offset, length] = parts.as_slice() else {
                            return Err("invalid slice".into());
                        };
                        requests.push(TransferRequest {
                            local_address: buffers["local"].memory.base_address()
                                + *local_offset as u64,
                            remote_address: buffers["source"].memory.base_address()
                                + *remote_offset as u64,
                            length: *length,
                        });
                    }
                }
                let operation = if *operation == "READ" {
                    TransferOp::Read
                } else {
                    TransferOp::Write
                };
                let report = if *name == "~" {
                    requester.submit_and_wait(operation, peer, &requests, TIMEOUT)?
                } else {
                    requester.submit_and_notify(
                        operation,
                        peer,
                        &requests,
                        &Notification {
                            name: text(name),
                            message: text(message),
                        },
                        TIMEOUT,
                    )?
                };
                if !report.is_complete() {
                    return Err(format!("transfer {label} failed: {report:?}").into());
                }
                if *name != "~"
                    && !requests.is_empty()
                    && !matches!(report.notification, NotificationStatus::Delivered)
                {
                    return Err(
                        format!("notification {label} failed: {:?}", report.notification).into(),
                    );
                }
                println!(
                    "OBS\tRESULT\t{label}\tcompleted\t{}\t{}",
                    report.completed_requests, report.transferred_bytes
                );
            }
            ["NOTIFY", label, mode, name, message] => {
                let notification = Notification {
                    name: text(name),
                    message: text(message),
                };
                match *mode {
                    "local" => source.send_local_notification(&notification)?,
                    "remote" => requester.send_notification(
                        peer.as_ref().ok_or("no peer")?,
                        &notification,
                        TIMEOUT,
                    )?,
                    _ => return Err("unknown notification route".into()),
                }
                println!("OBS\tNOTIFY\t{label}\tok");
            }
            ["TAKE", label] => {
                let messages = source.receive_notifications()?;
                let payload = messages
                    .iter()
                    .map(|message| {
                        format!(
                            "{}:{}",
                            hex(message.name.as_bytes()),
                            hex(message.message.as_bytes())
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(",");
                println!(
                    "OBS\tTAKE\t{label}\t{}\t{}",
                    messages.len(),
                    if payload.is_empty() { "-" } else { &payload }
                );
            }
            _ => {
                return Err(format!("invalid trace instruction at line {}", line_index + 1).into());
            }
        }
    }
    if buffers.values().any(|buffer| buffer.references != 0) {
        return Err("trace did not unregister all references".into());
    }
    source.close_source(TIMEOUT)?;
    requester.close_source(TIMEOUT)?;
    println!("OBS\tCLEANUP\tok");
    Ok(())
}
