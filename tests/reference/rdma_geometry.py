"""Compare valid Rust RDMA plans with the isolated, unchanged frozen TENT function."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import subprocess

REPO = Path(__file__).resolve().parents[2]
TEST = "transport::rdma::slice::tests::frozen_tent_slice_geometry_bounds_count_and_folds_only_short_tails"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tent-source", type=Path, required=True)
    parser.add_argument("--binary", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    output = args.output.resolve()
    if output.is_relative_to(REPO) or output.is_relative_to(args.tent_source.resolve()):
        parser.error("evidence must be outside both source checkouts")
    output.mkdir(parents=True, exist_ok=False)
    contract = json.loads((REPO / "tests/reference/contract.json").read_text())
    revision = contract["tent_revision"]
    name = "mooncake-transfer-engine/tent/include/tent/transport/rdma/slice.h"
    header = subprocess.check_output(["git", "show", revision + ":" + name], cwd=args.tent_source)
    text = header.decode()
    fragment = text[text.index("struct RdmaSlicePlan {"):text.index("// Forward declarations")]
    (output / "frozen-slice.h").write_bytes(header)
    (output / "frozen-function.inc").write_text(fragment)
    prefix = "#include <cstdint>\n#include <iostream>\nnamespace mooncake::tent {\n"
    suffix = r'''
}
int main() {
    uint64_t length, base, cap;
    while (std::cin >> length >> base >> cap) {
        const auto plan = mooncake::tent::planRdmaSlices(length, base, cap);
        std::cout << "ORBITTE_RDMA_GEOMETRY " << length << ' ' << base << ' ' << cap
                  << ' ' << plan.block_size << ' ' << plan.count << '\n';
    }
    return std::cin.eof() ? 0 : 1;
}
'''
    records = []
    manifest = dict(status="running", reference_commit=revision, reference_path=name,
                    header_sha256=hashlib.sha256(header).hexdigest(),
                    isolated_function_sha256=hashlib.sha256(fragment.encode()).hexdigest(),
                    native_transport_executed=False, native_function_executed=False,
                    performance_measurement=False, commands=records)

    def save():
        (output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")

    def run(label, command, input_bytes=None):
        result = subprocess.run(command, cwd=REPO, input=input_bytes, stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE, timeout=180)
        (output / (label + ".stdout")).write_bytes(result.stdout)
        (output / (label + ".stderr")).write_bytes(result.stderr)
        records.append(dict(label=label, command=command, returncode=result.returncode,
                            stdout_sha256=hashlib.sha256(result.stdout).hexdigest(),
                            stderr_sha256=hashlib.sha256(result.stderr).hexdigest()))
        save()
        result.check_returncode()
        return result.stdout.decode()

    try:
        command = ([str(args.binary.resolve()), TEST] if args.binary else
                   ["cargo", "test", "--locked", "--features", "rdma", "--lib", TEST, "--"])
        rust = run("rust-plans", command + ["--exact", "--nocapture"])
        expected = re.findall(r"^ORBITTE_RDMA_GEOMETRY [0-9 ]+$", rust, re.M)
        if len(expected) != 16 or len(set(expected)) != 16 or "test result: ok. 1 passed;" not in rust:
            raise ValueError("geometry selector must execute exactly one test and sixteen distinct plans")
        inputs = ("\n".join(" ".join(row.split()[1:4]) for row in expected) + "\n").encode()
        (output / "inputs.txt").write_bytes(inputs)
        compiler = shlex.split(os.environ.get("CXX", "g++"))
        run("compiler-version", compiler + ["--version"])
        controls = []
        for label, body in [
            ("native", fragment),
            ("quarter-inclusive", fragment.replace("static_cast<double>(tail) < merge_ratio", "static_cast<double>(tail) <= merge_ratio")),
            ("missing-fold", fragment.replace("double merge_ratio = 0.25", "double merge_ratio = 0.0")),
        ]:
            if label != "native" and body == fragment:
                raise ValueError("native geometry mutation did not apply")
            source = output / (label + ".cpp")
            source.write_text(text[:text.index("#ifndef")] + prefix + body + suffix)
            binary = output / label
            run(label + "-build", compiler + ["-O2", "-std=c++17", str(source), "-o", str(binary)])
            actual = run(label + "-plans", [str(binary)], inputs).splitlines()
            if label == "native":
                if actual != expected:
                    raise ValueError("Rust plans differ from the frozen native geometry function")
                manifest.update(native_function_executed=True, observations=len(actual),
                                native_binary_sha256=hashlib.sha256(binary.read_bytes()).hexdigest())
            else:
                if actual == expected:
                    raise ValueError("comparator accepted the native geometry mutation: " + label)
                controls.append(dict(name=label, status="rejected", binary_sha256=hashlib.sha256(binary.read_bytes()).hexdigest()))
        manifest.update(status="passed", controls=controls)
    except Exception as error:
        manifest.update(status="failed", error=str(error))
        raise
    finally:
        save()
    print(json.dumps({key: manifest[key] for key in ["status", "observations", "native_function_executed", "native_transport_executed", "controls"]}))


if __name__ == "__main__":
    main()
