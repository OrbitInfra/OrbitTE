//! Installed CUDA source for the ordinary TCP CPU-requester gate.
use orbit_te::{EngineConfig, Permission, TransferEngine, cuda::CudaMemory};
use std::{
    error::Error,
    io::{self, Write},
    time::Duration,
};

fn main() -> Result<(), Box<dyn Error>> {
    if std::env::args().nth(1).as_deref() == Some("--protocol") {
        println!("orbitte-peer-gpu-source");
        return Ok(());
    }
    let gpu = CudaMemory::allocate(0, 12)?;
    gpu.write(0, &[0xa5, 0xa5, 1, 2, 3, 4, 5, 6, 7, 8, 0xa5, 0xa5])?;
    let engine = TransferEngine::new(EngineConfig::default())?;
    let _regions =
        engine.register_cuda_memory_batch(&[(gpu.clone(), 0, 12)], Permission::RemoteReadWrite)?;
    println!("{} {}", engine.endpoint(), engine.source_epoch());
    io::stdout().flush()?;
    let mut line = String::new();
    io::stdin().read_line(&mut line)?;
    if line.trim() != "done" {
        return Err("requester did not finish".into());
    }
    engine.close(Duration::from_secs(30))?;
    let mut got = [0; 12];
    gpu.read(0, &mut got)?;
    if got != [0xa5, 0xa5, 8, 7, 6, 5, 4, 3, 2, 1, 0xa5, 0xa5] {
        return Err("CUDA payload or guards".into());
    }
    println!("gpu guards and WRITE verified");
    Ok(())
}
