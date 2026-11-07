"""Unrun installed EFA gate: two hosts, exact provider/domain, native byte evidence."""
import argparse
import hashlib
import ipaddress
import json
import selectors
import shlex
import subprocess
from pathlib import Path


def command(host, args):
    return ["ssh", "-o", "BatchMode=yes", "--", host, shlex.join(args)] if host else args


def run(host, args):
    return subprocess.run(command(host, args), check=True, text=True, capture_output=True, timeout=90).stdout


def counters(host, device):
    if not device or any(c not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-" for c in device):
        raise ValueError("invalid EFA sysfs device")
    code = "import json,pathlib; p=pathlib.Path(" + repr(f"/sys/class/infiniband/{device}/ports/1/hw_counters") + "); print(json.dumps({f.name:int(f.read_text()) for f in p.iterdir() if f.is_file()}))"
    values = json.loads(run(host, ["python3", "-c", code]))
    if not {"tx_bytes", "rx_bytes"} <= values.keys():
        raise ValueError("EFA byte counters are required for route evidence")
    return values


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--artifact", required=True, type=Path)
    ap.add_argument("--source-ssh", required=True)
    ap.add_argument("--source-ip", required=True)
    ap.add_argument("--requester-ip", required=True)
    ap.add_argument("--source-domain", required=True)
    ap.add_argument("--requester-domain", required=True)
    ap.add_argument("--source-device", required=True)
    ap.add_argument("--requester-device", required=True)
    ap.add_argument("--output", required=True, type=Path)
    a = ap.parse_args()
    for address in (a.source_ip, a.requester_ip):
        ip = ipaddress.ip_address(address)
        if ip.is_loopback or ip.is_unspecified:
            ap.error("two routed EFA hosts are required")
    artifact = a.artifact.resolve(strict=True)
    manifest = json.loads((artifact / "manifest.json").read_text())
    binary = str(artifact / "installed/bin/orbite-fabric")
    sha = hashlib.sha256(Path(binary).read_bytes()).hexdigest()
    if manifest["status"] != "passed" or manifest["dirty"] or manifest["binary_sha256"] != sha:
        raise ValueError("a clean packaged artifact is required")
    if run(a.source_ssh, ["sha256sum", binary]).split()[0] != sha:
        raise ValueError("install the identical artifact at the same path on both hosts")
    def snapshot():
        return {"source": counters(a.source_ssh, a.source_device), "requester": counters(None, a.requester_device)}
    evidence = {"manifest": manifest, "provider": {
        "source": run(a.source_ssh, ["env", "FI_PROVIDER=efa", "fi_info", "-p", "efa", "-t", "FI_EP_RDM"]),
        "requester": run(None, ["env", "FI_PROVIDER=efa", "fi_info", "-p", "efa", "-t", "FI_EP_RDM"])}, "before": snapshot()}
    source = subprocess.Popen(command(a.source_ssh, ["env", "FI_PROVIDER=efa", binary, "serve", a.source_ip, a.source_domain]), stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
    try:
        with selectors.DefaultSelector() as sel:
            sel.register(source.stdout, selectors.EVENT_READ)
            if not sel.select(60):
                raise TimeoutError("EFA source bootstrap")
        ready = source.stdout.readline().strip().split()
        if len(ready) != 3 or ready[0] != "READY":
            raise RuntimeError(f"source bootstrap: {ready}")
        evidence["requester"] = run(None, ["env", "FI_PROVIDER=efa", binary, "run", a.requester_ip, a.requester_domain, *ready[1:]])
        evidence["source"], _ = source.communicate("verify\n", timeout=60)
        if source.returncode or "PASS run" not in evidence["requester"] or "PASS serve" not in evidence["source"]:
            raise RuntimeError("missing guarded native completion")
        evidence["after"] = snapshot()
        for side in ("source", "requester"):
            for direction in ("tx_bytes", "rx_bytes"):
                if evidence["after"][side][direction] - evidence["before"][side][direction] < 2 << 20:
                    raise RuntimeError(f"insufficient native EFA traffic: {side}/{direction}")
        evidence["status"] = "passed"
        a.output.write_text(json.dumps(evidence, indent=2) + "\n")
    finally:
        if source.poll() is None:
            source.kill()
            source.wait(timeout=10)


if __name__ == "__main__":
    main()
