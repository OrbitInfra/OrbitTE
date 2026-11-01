#[cfg(not(feature = "rdma"))]
fn main() {}

#[cfg(feature = "rdma")]
fn main() {
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
