//! Fault one dedicated RoCE data link on the requester. The supervisor owns
//! the host locks through child exit and independently restores the link.
use super::*;
use orbite::{Error, TransportSelectorConfig};
use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
};

pub(super) fn policy(case: &Case) -> Result<TransportSelectorConfig> {
    Ok(TransportSelectorConfig::from_json(&format!(
        r#"{{"policy":[
        {{"name":"fault","segment_type":"memory","devices":["{}"],"transports":["rdma"]}},
        {{"name":"spare","segment_type":"memory","devices":["{}"],"transports":["rdma"]}}]}}"#,
        case.rails[0], case.rails[1]
    ))?)
}
struct Link {
    name: String,
    armed: bool,
}
fn set_link(name: &str, state: &str) -> Result<()> {
    if !Command::new("ip")
        .args(["link", "set", "dev", name, state])
        .status()?
        .success()
    {
        return Err("data-link change failed".into());
    }
    Ok(())
}
impl Link {
    fn down(case: &Case, control: &TcpStream) -> Result<Self> {
        let name = env::var("ORBITE_FAULT_NETDEV")?;
        let gid = env::var("ORBITE_SHARED_GID")?.parse::<u16>()?;
        let mapped = fs::read_to_string(format!(
            "/sys/class/infiniband/{}/ports/1/gid_attrs/ndevs/{gid}",
            case.rails[0]
        ))?;
        if name.is_empty()
            || mapped.trim() != name
            || !name
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || b"_-.".contains(&c))
        {
            return Err("netdev does not match the first locked RNIC GID".into());
        }
        let route = Command::new("ip")
            .args(["route", "get", &control.peer_addr()?.ip().to_string()])
            .output()?;
        if !route.status.success() {
            return Err("control route unavailable".into());
        }
        let route = std::str::from_utf8(&route.stdout)?;
        let mut words = route.split_whitespace();
        let control_net = words
            .find(|s| *s == "dev")
            .and_then(|_| words.next())
            .ok_or("control route has no netdev")?;
        if fs::read_dir(Path::new("/sys/class/net").join(&name))?
            .filter_map(|e| e.ok())
            .any(|e| e.file_name().to_string_lossy().starts_with("upper_"))
        {
            return Err("data netdev has dependent network interfaces".into());
        }
        if name == control_net {
            return Err("fault netdev carries the control connection".into());
        }
        let flags = fs::read_to_string(Path::new("/sys/class/net").join(&name).join("flags"))?;
        if u64::from_str_radix(flags.trim().trim_start_matches("0x"), 16)? & 1 == 0 {
            return Err("data link was not administratively up".into());
        }
        let root = PathBuf::from(env::var("ORBITE_SHARED_MAPS")?);
        fs::write(root.with_file_name("rail-link-armed"), &name)?;
        let link = Self { name, armed: true };
        set_link(&link.name, "down")?;
        Ok(link)
    }
    fn restore(&mut self) -> Result<()> {
        set_link(&self.name, "up")?;
        self.armed = false;
        Ok(())
    }
}
impl Drop for Link {
    fn drop(&mut self) {
        if self.armed {
            let _ = set_link(&self.name, "up");
        }
    }
}
fn await_condition(mut ready: impl FnMut() -> Result<bool>) -> Result<()> {
    let until = Instant::now() + TIMEOUT;
    while !ready()? {
        if Instant::now() >= until {
            return Err("rail state deadline".into());
        }
        thread::sleep(Duration::from_millis(10));
    }
    Ok(())
}

pub(super) fn run(
    case: &Case,
    engine: &TransferEngine,
    memory: &RegisteredMemory,
    peer: &RemoteSegment,
    control: &mut TcpStream,
) -> Result<()> {
    let original: Vec<_> = (0..memory.len())
        .map(|i| if case.source { (i % 191) as u8 } else { 0xee })
        .collect();
    memory.write(0, &original)?;
    barrier(control, "health-ready")?;
    // The source must outlive the requester's physical link transition.
    control.set_read_timeout(Some(TIMEOUT * 5))?;
    if !case.source {
        let transfer = |policy: &str, offset: usize| {
            engine.submit_and_wait(
                TransferOp::Read,
                peer,
                &[TransferRequest::new(
                    Transport::Rdma,
                    memory.base_address() + offset as u64,
                    peer.regions()[0].base_address() + offset as u64,
                    32,
                )
                .with_policy_name(policy)?],
                TIMEOUT,
            )
        };
        let stats = || {
            engine
                .stats()?
                .rdma
                .ok_or(Error::Unavailable("missing RDMA counters"))
        };
        if !transfer("fault", 16)?.is_complete() {
            return Err("native fault-rail warmup failed".into());
        }
        let before = stats()?;
        let mut link = Link::down(case, control)?;
        await_condition(|| Ok(stats()?.open_rails > before.open_rails))?;
        let blocked = transfer("fault", 64);
        let error = match blocked {
            Err(e) => e,
            Ok(r) => r.failure.ok_or("transfer used the disabled RNIC")?.error,
        };
        if !matches!(error, Error::TransportPolicy { .. } | Error::Unavailable(_))
            || stats()?.post_calls != before.post_calls
        {
            return Err("disabled rail posted payload".into());
        }
        if !transfer("spare", 112)?.is_complete() {
            return Err("healthy RNIC could not carry data during cooldown".into());
        }
        link.restore()?;
        await_condition(|| {
            Ok(engine
                .nic_load_stats()?
                .iter()
                .any(|n| n.device_name == case.rails[0]))
        })?;
        // Let the physical port stabilize and the cooldown expire without a trial.
        thread::sleep(Duration::from_secs(3));
        if !transfer("fault", 160)?.is_complete() {
            return Err("half-open RNIC failed its trial".into());
        }
        let after = stats()?;
        if after.rail_probes != before.rail_probes + 1
            || after.rail_recoveries != before.rail_recoveries + 1
            || after.open_rails != before.open_rails
        {
            return Err("rail recovery lacks a unique successful probe receipt".into());
        }
        let mut expected = original.clone();
        for i in (16..48).chain(112..144).chain(160..192) {
            expected[i] = (i % 191) as u8;
        }
        let mut got = vec![0; expected.len()];
        memory.read(0, &mut got)?;
        if got != expected {
            return Err("requester payload or guard mismatch".into());
        }
        let nics = engine.nic_load_stats()?;
        let completed = |name: &str| -> Result<u64> {
            Ok(nics
                .iter()
                .find(|n| n.device_name == name)
                .ok_or("missing native NIC evidence")?
                .completed_bytes)
        };
        let first = completed(&case.rails[0])?;
        let spare = completed(&case.rails[1])?;
        if (first, spare) != (64, 32) {
            return Err("native byte counts do not match the two locked routes".into());
        }
        println!(
            "ORBITE_RAIL_HEALTH {{\"role\":\"requester\",\"blocked_posts\":0,\"probes\":{},\"recoveries\":{},\"fault_rail_bytes\":{},\"spare_rail_bytes\":{},\"verified_bytes\":{}}}",
            after.rail_probes - before.rail_probes,
            after.rail_recoveries - before.rail_recoveries,
            first,
            spare,
            got.len()
        );
    }
    barrier(control, "health-complete")?;
    if case.source {
        let mut got = vec![0; original.len()];
        memory.read(0, &mut got)?;
        if got != original {
            return Err("source registration changed".into());
        }
        println!(
            "ORBITE_RAIL_HEALTH {{\"role\":\"source\",\"verified_bytes\":{}}}",
            got.len()
        );
    }
    Ok(())
}
