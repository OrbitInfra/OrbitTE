"""Parse the real export from a public consumer with the Prometheus reference client."""
import json
import subprocess
import sys
from urllib.request import urlopen

from prometheus_client.parser import text_string_to_metric_families

with subprocess.Popen([sys.argv[1], "serve"], stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True) as peer:
    try:
        port = int(peer.stdout.readline())
        def fetch(path):
            with urlopen(f"http://127.0.0.1:{port}{path}", timeout=5) as response:
                return response.read().decode()

        samples = [sample for family in text_string_to_metric_families(fetch("/metrics"))
                   for sample in family.samples]
        def reading(name, labels):
            values = [s.value for s in samples if s.name == name and s.labels == labels]
            assert len(values) == 1, (name, labels, values)
            return values[0]

        labels = {"transport": "tcp"}
        assert reading("tent_read_bytes_total", labels) == 64
        assert reading("tent_read_requests_total", labels) == 1
        assert reading("tent_transport_attempts_total", {**labels, "operation": "read"}) == 1
        for name in ["tent_read_latency_us", "tent_read_size_bytes", "tent_stage_queue_wait_us"]:
            assert reading(name + "_count", labels) == reading(name + "_bucket", {**labels, "le": "+Inf"}) == 1
        assert json.loads(fetch("/metrics/json"))["tent_read_size_bytes"]["count"] == 1
        assert fetch("/health") == "OK"
        print(f"Parsed {len(samples)} samples from the loopback TCP consumer")
    finally:
        peer.communicate(input="done\n", timeout=10)
    assert peer.returncode == 0
