"""Check an uneven HP-TCP READ and single-lane WRITE across two independent processes."""
import argparse
import ipaddress
import json
import selectors
import shlex
import subprocess
import sys


def run(argv, **options):
    return subprocess.run(argv, check=True, timeout=60, text=True, **options)


def host_command(host, argv):
    return ["ssh", "-o", "BatchMode=yes", "--", host, shlex.join(argv)] if host else argv


def rail_counters(host, rails, peer_rails):
    result = {}
    for local, peer in zip(rails, peer_rails):
        route = json.loads(run(host_command(host, ["ip", "-j", "route", "get", peer, "from", local]),
                               capture_output=True).stdout)[0]
        dev = route["dev"]
        if dev == "lo" or dev in result:
            raise RuntimeError("the physical gate needs distinct routed interfaces")
        counters = run(host_command(host, ["cat", f"/sys/class/net/{dev}/statistics/rx_bytes",
                                          f"/sys/class/net/{dev}/statistics/tx_bytes"]), capture_output=True)
        result[dev] = list(map(int, counters.stdout.split()))
    return result


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--binary", required=True, help="same installed binary path on both hosts")
    mode = ap.add_mutually_exclusive_group(required=True)
    mode.add_argument("--loopback", action="store_true")
    mode.add_argument("--source-ssh")
    ap.add_argument("--source-rails")
    ap.add_argument("--requester-rails")
    args = ap.parse_args()
    source, requester = (["127.0.0.1", "127.0.0.2"], ["127.0.0.3", "127.0.0.4"]) if args.loopback else (
        (args.source_rails or "").split(","), (args.requester_rails or "").split(","))
    for ips in [source, requester]:
        if len(set(ips)) != 2 or len(ips) != 2 or any(ipaddress.IPv4Address(s).is_loopback != args.loopback for s in ips):
            ap.error("supply two distinct IPv4 rails per host for this mode")
    def snapshot():
        return {} if args.loopback else {
            "source": rail_counters(args.source_ssh, source, requester),
            "requester": rail_counters(None, requester, source)}
    before = snapshot()
    source_proc = subprocess.Popen(host_command(args.source_ssh, [args.binary, "serve", ",".join(source)]),
                                   stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
    try:
        with selectors.DefaultSelector() as sel:
            sel.register(source_proc.stdout, selectors.EVENT_READ)
            if not sel.select(45):
                raise TimeoutError("source bootstrap")
        ready = source_proc.stdout.readline().strip().split()
        if len(ready) != 3 or ready[0] != "READY":
            raise RuntimeError(f"source bootstrap failed: {ready}")
        out = run([args.binary, "run", ",".join(requester), *ready[1:]], capture_output=True)
        if "PASS run 2097155" not in out.stdout:
            raise RuntimeError(out.stdout)
        receipt, _ = source_proc.communicate("verify\n", timeout=45)
        if source_proc.returncode or "PASS serve 2097155" not in receipt:
            raise RuntimeError(f"source receipt: {receipt}")
        after = snapshot()
        for side, devices in after.items():
            for dev, (rx, tx) in devices.items():
                # Each half of the READ crosses its statically paired interface.
                direction = 1 if side == "source" else 0
                if (rx, tx)[direction] - before[side][dev][direction] < 1 << 20:
                    raise RuntimeError(f"missing READ traffic on {side}/{dev}")
        print(json.dumps({"profile": "loopback" if args.loopback else "physical",
                          "bytes_each_direction": 2097155, "guards": "passed",
                          "before": before, "after": after}))
    finally:
        if source_proc.poll() is None:
            source_proc.kill()
            source_proc.wait(timeout=10)


if __name__ == "__main__":
    main()
