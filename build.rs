fn main() {
    build_identity();
    println!("cargo:rustc-check-cfg=cfg(unsupported_native_target)");
    if std::env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("linux") {
        #[cfg(feature = "rdma")]
        build_port_shim();
    } else if cfg!(any(
        feature = "cuda",
        feature = "rdma",
        feature = "io-uring",
        feature = "gds"
    )) {
        println!("cargo:rustc-cfg=unsupported_native_target");
    }
}

// Build identity is diagnostic metadata only. Neither the wire nor feature selection uses it.
fn build_identity() {
    use std::{env, fs, path::Path, process::Command};
    let git = |args: &[&str]| {
        let output = Command::new("git").args(args).output().ok()?;
        output
            .status
            .success()
            .then(|| String::from_utf8_lossy(&output.stdout).trim().to_owned())
    };
    let commit = if Path::new(".cargo_vcs_info.json").is_file() {
        println!("cargo:rerun-if-changed=.cargo_vcs_info.json");
        fs::read_to_string(".cargo_vcs_info.json")
            .ok()
            .and_then(|v| {
                let (_, value) = v.split_once("\"sha1\"")?;
                Some(value.split('"').nth(1)?.to_owned())
            })
    } else if Path::new(".git").exists() {
        for name in [
            Some("HEAD".into()),
            git(&["symbolic-ref", "-q", "HEAD"]),
            Some("packed-refs".into()),
        ]
        .into_iter()
        .flatten()
        {
            if let Some(path) = git(&["rev-parse", "--git-path", &name]) {
                if Path::new(&path).exists() {
                    println!("cargo:rerun-if-changed={path}");
                }
            }
        }
        git(&["rev-parse", "--verify", "HEAD"])
    } else {
        None
    };
    let commit = commit.filter(|s| s.len() == 40 && s.bytes().all(|b| b.is_ascii_hexdigit()));
    println!(
        "cargo:rustc-env=ORBITTE_BUILD_COMMIT={}",
        commit.as_deref().unwrap_or("unknown")
    );
    println!(
        "cargo:rustc-env=ORBITTE_BUILD_PROFILE={}",
        env::var("PROFILE").unwrap_or_default()
    );
    let compiler = env::var_os("RUSTC")
        .and_then(|c| Command::new(c).arg("-V").output().ok())
        .filter(|o| o.status.success())
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string());
    println!(
        "cargo:rustc-env=ORBITTE_BUILD_COMPILER={}",
        compiler.as_deref().unwrap_or("unknown")
    );
}

#[cfg(feature = "rdma")]
fn build_port_shim() {
    use std::{env, fs, path::PathBuf};

    const HEADER: &str = "src/transport/rdma/sys/port.h";
    const SOURCE: &str = "src/transport/rdma/sys/port.c";
    println!("cargo:rerun-if-changed={HEADER}");
    println!("cargo:rerun-if-changed={SOURCE}");
    let output = PathBuf::from(env::var_os("OUT_DIR").expect("Cargo supplies OUT_DIR"));
    let probe = output.join("port_speed_ex.c");
    fs::write(
        &probe,
        "#include <infiniband/verbs.h>\nint orbitte_speed_ex(struct ibv_port_attr *a) { return a->active_speed_ex; }\n",
    )
    .expect("write port capability probe");
    let mut native = cc::Build::new();
    let compiler = native.get_compiler();
    let extended = compiler
        .to_command()
        .arg("-c")
        .arg(&probe)
        .arg("-o")
        .arg(output.join("port_speed_ex.o"))
        .output()
        .expect("run port capability probe")
        .status
        .success();
    if extended {
        native.define("ORBITTE_HAVE_ACTIVE_SPEED_EX", None);
    }
    println!("cargo:rustc-env=ORBITTE_RDMA_ACTIVE_SPEED_EX={extended}");
    native.file(SOURCE).compile("orbitte_port");
    bindgen::Builder::default()
        .header(HEADER)
        .allowlist_function("orbitte_query_port")
        .allowlist_type("orbitte_port_attr")
        .derive_default(true)
        .parse_callbacks(Box::new(bindgen::CargoCallbacks::new()))
        .generate()
        .expect("generate the port shim ABI")
        .write_to_file(output.join("port.rs"))
        .expect("write the port shim ABI");
}
