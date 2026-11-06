use serde_json::Value;
use std::process::Command;

#[test]
fn packaged_cli_contract_and_compiled_feature_flags() {
    for (args, status, stdout, stderr) in [
        (vec!["--help"], 0, "diagnostics", ""),
        (vec!["diagnostics", "--help"], 0, "version", ""),
        (
            vec!["diagnostics", "version"],
            0,
            "build.info.available",
            "",
        ),
        (
            vec!["diagnostics", "version", "--json"],
            0,
            "\"command\": \"version\"",
            "",
        ),
        (vec![], 2, "", "tent [OPTIONS]"),
        (vec!["unknown"], 2, "", "tent [OPTIONS]"),
        (vec!["version"], 2, "", "tent [OPTIONS]"),
        (vec!["diagnostics"], 2, "", "diagnostics [OPTIONS]"),
        (
            vec!["diagnostics", "unknown"],
            2,
            "",
            "tent diagnostics [OPTIONS]",
        ),
        (
            vec!["diagnostics", "version", "--bad"],
            2,
            "",
            "tent diagnostics version",
        ),
        (
            vec!["diagnostics", "version", "--json", "extra"],
            2,
            "",
            "tent diagnostics version",
        ),
    ] {
        let output = Command::new(env!("CARGO_BIN_EXE_tent"))
            .args(&args)
            .env("TENT_METRICS_HTTP_HOST", "invalid.invalid")
            .env("TENT_METRICS_HTTP_PORT", "invalid")
            .env("CUDA_VISIBLE_DEVICES", "")
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(status), "{args:?}");
        for (bytes, needle) in [
            (output.stdout.as_slice(), stdout),
            (output.stderr.as_slice(), stderr),
        ] {
            let text = std::str::from_utf8(bytes).unwrap();
            if needle.is_empty() {
                assert!(text.is_empty(), "{text}");
            } else {
                assert!(text.contains(needle), "{text}");
            }
        }
        if args.last() == Some(&"--json") {
            let doc: Value = serde_json::from_slice(&output.stdout).unwrap();
            for (name, active) in [
                ("rdma", cfg!(feature = "rdma")),
                ("cuda", cfg!(feature = "cuda")),
                ("uring", cfg!(feature = "io-uring")),
                ("metrics", cfg!(feature = "metrics")),
                ("device-plugin", cfg!(feature = "device-plugin")),
                ("rdma-adaptive", cfg!(feature = "rdma-adaptive")),
                ("gds", cfg!(feature = "gds")),
            ] {
                assert_eq!(doc["build"]["features"][name], active);
            }
        }
    }
}
