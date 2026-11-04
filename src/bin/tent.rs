//! Offline diagnostic command. All collection and rendering lives in the library.
use orbit_te::diagnostics::version_snapshot;
use std::{
    ffi::OsString,
    io::{self, Write},
    process::ExitCode,
};

const ROOT: &str = "tent [OPTIONS] diagnostics\n  diagnostics    Run offline diagnostics";
const DIAGNOSTICS: &str =
    "tent diagnostics [OPTIONS] version\n  version        Show build and feature information";
const VERSION: &str = "tent diagnostics version [--json]\n  --json         Emit JSON";

fn run(mut args: impl Iterator<Item = OsString>) -> u8 {
    let help = |s: &str| matches!(s, "-h" | "--help");
    let usage = |s: &str| {
        let _ = writeln!(io::stderr().lock(), "{s}");
        2
    };
    for (expected, message) in [("diagnostics", ROOT), ("version", DIAGNOSTICS)] {
        match args.next().as_deref().and_then(|s| s.to_str()) {
            Some(s) if help(s) => return output(message),
            Some(s) if s == expected => {}
            _ => return usage(message),
        }
    }
    let json = match args.next() {
        None => false,
        Some(v) if v == "--json" => true,
        Some(v) if v.to_str().is_some_and(help) => return output(VERSION),
        _ => return usage(VERSION),
    };
    if args.next().is_some() {
        return usage(VERSION);
    }
    let snapshot = version_snapshot();
    let document = if json {
        let Ok(text) = snapshot.render_json() else {
            return 4;
        };
        text
    } else {
        snapshot.render_text()
    };
    let status = output(document.trim_end());
    if status == 0 {
        snapshot.exit_code(false)
    } else {
        status
    }
}
fn output(s: &str) -> u8 {
    if writeln!(io::stdout().lock(), "{s}").is_ok() {
        0
    } else {
        4
    }
}
fn main() -> ExitCode {
    run(std::env::args_os().skip(1)).into()
}
