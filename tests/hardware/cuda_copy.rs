//! Explicit GPU copy gate; no GPU access during ordinary Cargo tests.

use std::env;
use std::sync::Arc;
use std::thread;

use orbit_te::cuda::{CudaMemory, retained_resources};
use orbit_te::{Error, Result};

const GUARD: usize = 67;
const WORKERS: usize = 8;

fn exercise(device: i32, payload: usize) -> Result<()> {
    let stride = payload + 2 * GUARD;
    let length = WORKERS * stride;
    let memory = Arc::new(CudaMemory::allocate(device, length)?);
    memory.write(0, &vec![0xa5; length])?;
    let mut handles = Vec::new();
    for lane in 0..WORKERS {
        let memory = Arc::clone(&memory);
        handles.push(thread::spawn(move || -> Result<()> {
            for round in 0..4 {
                let expected: Vec<u8> = (0..payload)
                    .map(|index| ((index * 37 + lane * 11 + round * 53) % 251) as u8)
                    .collect();
                let offset = lane * stride + GUARD;
                memory.write(offset, &expected)?;
                let mut actual = vec![0; payload];
                memory.read(offset, &mut actual)?;
                if actual != expected {
                    return Err(Error::Internal("CUDA concurrent copy byte mismatch"));
                }
            }
            Ok(())
        }));
    }
    // Join every submitted thread before returning any error.
    let mut result = Ok(());
    for handle in handles {
        let outcome = handle
            .join()
            .unwrap_or(Err(Error::Internal("CUDA copy worker panicked")));
        if result.is_ok() {
            result = outcome;
        }
    }
    result?;
    let mut actual = vec![0; length];
    memory.read(0, &mut actual)?;
    for lane in 0..WORKERS {
        let start = lane * stride;
        if actual[start..start + GUARD].iter().any(|b| *b != 0xa5)
            || actual[start + GUARD + payload..start + stride]
                .iter()
                .any(|b| *b != 0xa5)
        {
            return Err(Error::Internal("CUDA copy guard changed"));
        }
    }
    drop(memory);
    let retained = retained_resources();
    if retained.device_allocations != 0
        || retained.pinned_allocations != 0
        || retained.contexts != 0
        || retained.streams != 0
        || retained.events != 0
    {
        return Err(Error::Internal("CUDA copy resources retained"));
    }
    println!(
        "CUDA_COPY_OK device={device} payload={payload} workers={WORKERS} rounds=4 bytes={length} retained=0"
    );
    Ok(())
}

fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = env::args().skip(1).collect();
    if args.is_empty() {
        return Err("usage: cuda_copy <device ordinal> [device ordinal ...]".into());
    }
    for ordinal in args {
        let device = ordinal.parse()?;
        for payload in [17, 4096 + 37, (1 << 20) + 19] {
            exercise(device, payload)?;
        }
    }
    Ok(())
}
