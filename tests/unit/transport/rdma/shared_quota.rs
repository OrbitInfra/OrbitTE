use super::*;
use std::io::{BufRead, BufReader};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT: AtomicU64 = AtomicU64::new(0);

struct Object(CString);
impl Object {
    fn new() -> Self {
        Self(
            CString::new(format!(
                "/orbitte-slots-test-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ))
            .unwrap(),
        )
    }
    fn name(&self) -> &str {
        self.0.to_str().unwrap()
    }
}
impl Drop for Object {
    fn drop(&mut self) {
        unsafe {
            libc::shm_unlink(self.0.as_ptr());
        }
    }
}

#[test]
fn shared_slots_validate_identity_and_rotate_without_a_background_thread() {
    let object = Object::new();
    let interval = Duration::from_millis(2);
    let owner = SharedSlotManager::attach(object.name(), interval).unwrap();
    let peer = SharedSlotManager::attach(object.name(), interval).unwrap();
    for now in [
        0,
        1_999_999,
        2_000_000,
        3_999_999,
        4_000_000,
        5_999_999,
        6_000_000,
        u64::MAX,
    ] {
        let expected =
            [Priority::High, Priority::Medium, Priority::Low][((now / 2_000_000) % 3) as usize];
        assert_eq!(owner.slot_at(now), peer.slot_at(now));
        assert_eq!(
            owner.slot_at(now),
            (expected, Duration::from_nanos(2_000_000 - now % 2_000_000))
        );
    }
    assert!(matches!(
        SharedSlotManager::attach(object.name(), Duration::from_millis(3)),
        Err(Error::InvalidConfig(_))
    ));
    let mut header = [0; HEADER_SIZE as usize];
    owner._object.read_exact_at(&mut header, 0).unwrap();
    header[24] ^= 1;
    owner._object.write_all_at(&header, 0).unwrap();
    assert!(matches!(
        SharedSlotManager::attach(object.name(), interval),
        Err(Error::InvalidConfig(_))
    ));
    header[24] ^= 1;
    owner._object.write_all_at(&header, 0).unwrap();
    drop(owner);
    drop(peer);
    SharedSlotManager::attach(object.name(), interval).unwrap();
    for name in ["", "/", "relative", "/a/b", "/..", "/nul\0"] {
        assert!(
            SharedSlotManager::validate(name, interval).is_err(),
            "{name:?}"
        );
    }
    assert!(SharedSlotManager::validate(object.name(), Duration::ZERO).is_err());
}

fn child(name: &str, mode: &str) -> std::process::Child {
    Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "transport::rdma::shared_quota::tests::shared_slot_subprocess",
            "--ignored",
            "--nocapture",
        ])
        .env("ORBITTE_TEST_SLOT_NAME", name)
        .env("ORBITTE_TEST_SLOT_MODE", mode)
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap()
}

#[test]
fn independent_process_attach_and_killed_initializer_recovery() {
    let object = Object::new();
    let mut initializing = child(object.name(), "initialize");
    let mut stdout = BufReader::new(initializing.stdout.take().unwrap());
    let mut line = String::new();
    loop {
        assert!(
            stdout.read_line(&mut line).unwrap() != 0,
            "initializer exited before taking lock: {line}"
        );
        if line.contains("SLOT_INITIALIZING") {
            break;
        }
        line.clear();
    }
    initializing.kill().unwrap();
    initializing.wait().unwrap();
    let owner = SharedSlotManager::attach(object.name(), Duration::from_millis(2)).unwrap();
    let mut children: Vec<_> = (0..4).map(|_| child(object.name(), "attach")).collect();
    for process in &mut children {
        assert!(process.wait().unwrap().success());
    }
    // A conflicting join never modifies an already accepted header.
    assert!(child(object.name(), "mismatch").wait().unwrap().success());
    owner.current_slot().unwrap();
    drop(owner);
    assert!(child(object.name(), "attach").wait().unwrap().success());
}

#[test]
#[ignore = "subprocess fixture"]
fn shared_slot_subprocess() {
    let name = std::env::var("ORBITTE_TEST_SLOT_NAME").unwrap();
    let mode = std::env::var("ORBITTE_TEST_SLOT_MODE").unwrap();
    if mode == "initialize" {
        let name = CString::new(name).unwrap();
        let fd = unsafe { libc::shm_open(name.as_ptr(), libc::O_RDWR | libc::O_CREAT, 0o600) };
        assert!(fd >= 0);
        let file = unsafe { File::from_raw_fd(fd) };
        assert_eq!(unsafe { libc::flock(fd, libc::LOCK_EX) }, 0);
        file.set_len(HEADER_SIZE).unwrap();
        file.write_all_at(&999u64.to_le_bytes(), 8).unwrap();
        println!("SLOT_INITIALIZING");
        loop {
            thread::park();
        }
    }
    let interval = Duration::from_millis(if mode == "mismatch" { 3 } else { 2 });
    if mode == "mismatch" {
        assert!(matches!(
            SharedSlotManager::attach(&name, interval),
            Err(Error::InvalidConfig(_))
        ));
        return;
    }
    let slots = SharedSlotManager::attach(&name, interval).unwrap();
    slots.current_slot().unwrap();
}
