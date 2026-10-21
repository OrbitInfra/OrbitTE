use std::env;
use std::path::PathBuf;
use std::process::Command;

fn main() {
    println!("cargo:rerun-if-changed=native/verbs.c");
    println!("cargo:rerun-if-env-changed=CC");
    println!("cargo:rerun-if-env-changed=AR");
    if env::var_os("CARGO_FEATURE_RDMA").is_none() {
        return;
    }
    assert_eq!(
        env::var("CARGO_CFG_TARGET_OS").as_deref(),
        Ok("linux"),
        "RDMA requires Linux libibverbs"
    );
    assert_eq!(
        env::var("HOST"),
        env::var("TARGET"),
        "RDMA cross compilation requires a target-specific sys build (not implemented)"
    );
    let out = PathBuf::from(env::var_os("OUT_DIR").expect("Cargo OUT_DIR"));
    let compiler = env::var_os("CC").unwrap_or_else(|| "cc".into());
    let archiver = env::var_os("AR").unwrap_or_else(|| "ar".into());
    let status = Command::new(compiler)
        .args([
            "-std=c11",
            "-O2",
            "-fPIC",
            "-Wall",
            "-Wextra",
            "-Werror",
            "-c",
            "native/verbs.c",
            "-o",
        ])
        .arg(out.join("verbs.o"))
        .status()
        .expect("run C compiler for the libibverbs inline ABI bridge");
    assert!(
        status.success(),
        "libibverbs headers/compiler are required for --features rdma"
    );
    let status = Command::new(archiver)
        .arg("crs")
        .arg(out.join("liborbitte_verbs.a"))
        .arg(out.join("verbs.o"))
        .status()
        .expect("archive verbs ABI bridge");
    assert!(status.success(), "archive verbs ABI bridge");
    println!("cargo:rustc-link-search=native={}", out.display());
    println!("cargo:rustc-link-lib=static=orbitte_verbs");
    println!("cargo:rustc-link-lib=ibverbs");
}
