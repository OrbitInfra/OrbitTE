"""Validate the S16 acceptance index against the frozen upstream source tree."""

import argparse
import csv
import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import tempfile

from source import config_surface, lifecycle

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[2]
TENT = "mooncake-transfer-engine/tent"


def index(filename, column):
    with (HERE / (filename + ".tsv")).open() as f:
        rows = csv.DictReader(f, delimiter="\t", quoting=csv.QUOTE_NONE)
        by_key = {}
        for r in rows:
            if None in r or not all(r.values()) or r[column] in by_key:
                raise ValueError(f"{filename}:{rows.line_num}: missing field or duplicate {column}")
            by_key[r[column]] = r
        return by_key


def equal(label, actual, wanted):
    if actual != wanted:
        raise ValueError(
            f"{label}: missing {sorted(wanted - actual)}, unexpected {sorted(actual - wanted)}"
        )


def path_at(root, reference):
    name, _, line = reference.partition(":")
    path = root / name
    if not path.exists() or (line and not (1 <= int(line) <= len(path.read_text().splitlines()))):
        raise ValueError(f"unresolved reference {reference}")
    return path


def check_links(tent):
    case = index("cases", "case")
    used = set()
    for row in case.values():
        path_at(REPO, row["owner"])
    for table, key in [
        ("c-api", "symbol"),
        ("rpc", "name"),
        ("config", "key"),
        ("metadata", "item"),
        ("environment", "variable"),
        ("metrics", "family"),
        ("observations", "item"),
        ("capabilities", "transport"),
        ("bindings", "surface"),
    ]:
        for row in index(table, key).values():
            if row["case"] not in case:
                raise ValueError(f"{table}: unknown acceptance case {row['case']}")
            used.add(row["case"])
            if "source" in row:
                path_at(tent, row["source"])
    equal("acceptance cases with rows", used, set(case))


def check_c_api(tent):
    api = (tent / "include/tent/transfer_engine.h").read_text()
    functions = {
        m[1]: " ".join(m[0].split())
        for m in re.finditer(
            r"^(?:int|void|tent_batch_id_t|tent_engine_t) (tent_\w+)\([^;{}]*\);", api, re.M
        )
    }
    c_rows = index("c-api", "symbol")
    equal("C header", set(c_rows), set(functions))
    if len(functions) != 33 or any(r["declaration"] != functions[k] for k, r in c_rows.items()):
        raise ValueError("frozen C signature or symbol count changed")


def gtest_filters(text):
    suites = set(re.findall(r"\bTEST(?:_F)?\s*\(\s*(\w+)\s*,", text))
    params = set(re.findall(r"\bTEST_P\s*\(\s*(\w+)\s*,", text))
    instances = re.findall(r"\bINSTANTIATE_TEST_SUITE_P\s*\(\s*(\w+)\s*,\s*(\w+)", text)
    equal("parameterized GTest instantiations", {suite for _, suite in instances}, params)
    return {s + ".*" for s in suites} | {f"{prefix}/{suite}.*" for prefix, suite in instances}


def check_tests(tent):
    tests = index("tests", "source")
    equal(
        "C++ test sources",
        set(tests),
        {str(p.relative_to(tent)) for p in (tent / "tests").rglob("*test.cpp")},
    )
    if len(tests) != 88:
        raise ValueError("unexpected reference test source count")
    for name, row in tests.items():
        equal(name, set(row["gtest_filter"].split(":")), gtest_filters((tent / name).read_text()))
        path_at(REPO, row["orbitte_owner_or_control"])


def check_rpcs(tent):
    rpc = (tent / "include/tent/rpc/rpc.h").read_text().split("enum RpcFuncID {")[1].split("};")[0]
    values = re.sub(r"//[^\n]*", "", rpc).split(",")
    ids = {}
    n = 0
    for value in filter(str.strip, values):
        name, sep, explicit = value.strip().partition("=")
        n = int(explicit) if sep else n + 1
        ids[name.strip()] = n
    mapped = {k: int(r["id"]) for k, r in index("rpc", "name").items()}
    if mapped != ids:
        raise ValueError("RPC names/IDs differ from RpcFuncID")
    equal(
        "RPC capture",
        set(json.loads((HERE.parent / "wire/rpc.json").read_text())),
        set(ids) | {"Unknown"},
    )


def check_capabilities(tent):
    caps = index("capabilities", "transport")
    native = (
        (tent / "include/tent/runtime/transport.h")
        .read_text()
        .split("struct Capabilities {")[1]
        .split("};")[0]
    )
    fields = set(re.findall(r"bool (\w+) = false;", native))
    equal(
        "capability fields",
        set(next(iter(caps.values()))) - {"transport", "value", "notify", "cancel", "source", "case"},
        fields,
    )
    types = (tent / "include/tent/common/types.h").read_text()
    names = dict(
        re.findall(
            r'case (\w+):\s*return "([^"]+)";', types.split("transportTypeName(")[1].split("inline ")[0]
        )
    )
    enum = types.split("enum TransportType : int {")[1].split("kNumTransportTypes")[0]
    members = re.findall(r"^\s*(\w+)(?:\s*=\s*0)?\s*,", enum, re.M)
    if {names[m]: i for i, m in enumerate(members)} != {k: int(r["value"]) for k, r in caps.items()}:
        raise ValueError("transport enum values or JSON names changed")

    api = (tent / "include/tent/transfer_engine.h").read_text()
    c_constants = {k.lower(): int(v) for k, v in re.findall(r"#define TRANSPORT_(\w+) \((\d+)\)", api)}
    for native_key, key in [("ascend_direct", "ascend"), ("iouring", "io_uring")]:
        c_constants[key] = c_constants.pop(native_key)
    if c_constants != {k: int(r["value"]) for k, r in caps.items()}:
        raise ValueError("C transport constants differ")
    for k, row in caps.items():
        if k == "unspec":
            continue
        path = tent / row["source"]
        code = path.read_text()
        assigned = set(re.findall(r"caps\.(\w+)\s*=\s*true;", code))
        equal(k + " direction flags", {f for f in fields if row[f] != "no"}, fields & assigned)
        provider_header = (
            tent / "include/tent" / Path(row["source"]).relative_to("src").with_suffix(".h")
        )
        decls = provider_header.read_text()
        for col, method in [("notify", "supportNotification"), ("cancel", "supportsCancellation")]:
            overrides = re.search(
                r"bool\s+" + method + r"\(\) const(?: override)?\s*\{\s*return true;", decls
            )
            if (row[col] == "yes") != bool(overrides):
                raise ValueError(f"{k}: {method} differs")


def check_config(tent):
    keys, variables = config_surface(tent)
    cfg = index("config", "key")
    equal("config source keys", set(cfg) - {k for k in cfg if k.startswith("qos/")}, set(keys))
    qos = (tent / "src/runtime/qos_contract.cpp").read_text()

    def declared(name):
        return re.findall(r'"([a-z_]+)"', qos.split(name + " = {")[1].split("};", 1)[0])

    nested = {"qos/" + leaf for leaf in declared("kAllowedQosKeys")}
    nested.update("qos/tenants[]/" + leaf for leaf in declared("kAllowedTenantKeys"))
    for scope in ("qos/defaults", "qos/tenants[]/defaults", "qos/tenants[]/intents/{intent}"):
        nested.update(scope + "/" + leaf for leaf in declared("kAllowedPolicyKeys"))
    equal("nested QoS contract", {k for k in cfg if k.startswith("qos/")}, nested)
    equal("environment spellings", set(index("environment", "variable")), set(variables))
    classify = lifecycle(tent)
    for key, row in cfg.items():
        if classify(key) != (row["lifecycle"], row["constructor_precedence"]):
            raise ValueError(f"{key}: configuration precedence or lifecycle changed")
    for key, reads in keys.items():
        # Direct literal defaults are independently checkable. Conditional defaults and
        # parameter-struct initializers also have an explicit effective-default row.
        literals = {
            value
            for _, _, value in reads
            if re.fullmatch(r'true|false|"[^"]*"|-?\d+(?:\.\d+)?(?:[eE]-?\d+)?', value)
        }
        if len(literals) == 1:
            literal = literals.pop()
            if not (cfg[key]["absent"] == literal or cfg[key]["absent"].startswith(literal + " ")):
                raise ValueError(f"{key}: default {cfg[key]['absent']} differs from {literal}")


def check_metrics(tent):
    families = index("metrics", "family")
    header = (tent / "include/tent/metrics/tent_metrics.h").read_text()
    renderer = (tent / "src/metrics/tent_metrics.cpp").read_text()
    equal(
        "TENT metric families", set(families), set(re.findall(r'"(tent_[a-z0-9_]+)"', header + renderer))
    )
    labels = {
        k: ",".join(re.findall(r'"(\w+)"', v))
        for k, v in re.findall(r"(k\w+Labels?)\s*\{([^}]+)};", header)
    }
    buckets = {
        k: ",".join(re.findall(r"\d+", v)) for k, v in re.findall(r"(k\w+Buckets)\s*\{([^}]+)};", header)
    }
    for name, row in families.items():
        kind = (
            "histogram"
            if row["finite_buckets"] != "-"
            else "counter"
            if name.endswith("_total")
            else "gauge"
        )
        if row["type"] != kind:
            raise ValueError(f"{name}: invalid family type")
        if name.startswith("tent_metrics_"):
            wanted = (
                "version,commit"
                if name.endswith("build_info")
                else "format"
                if "serialization" in name or "timestamp" in name
                else "-"
            )
        else:
            descriptor = header.split('"' + name + '"', 1)[1].split("};", 1)[0]
            wanted = next((v for k, v in labels.items() if k in descriptor), "-")
            if any(k in descriptor for k in buckets) != (kind == "histogram"):
                raise ValueError(f"{name}: histogram family differs")
        if wanted != row["labels"]:
            raise ValueError(f"{name}: label set differs")
        if row["finite_buckets"] != "-":
            entry = header.split('"' + name + '"', 1)[1].split("};", 1)[0]
            if (
                row["type"] != "histogram"
                or not any(k in entry and v == row["finite_buckets"] for k, v in buckets.items())
                or not any(k in entry and v == row["labels"] for k, v in labels.items())
            ):
                raise ValueError(f"{name}: labels or histogram boundaries changed")


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--source", type=Path, required=True, help="clean frozen Mooncake checkout")
    root = p.parse_args().source.resolve()
    contract = json.loads((HERE.parent / "contract.json").read_text())
    git = ["git", "-c", "safe.directory=" + str(root), "-C", str(root)]
    revision = subprocess.check_output([*git, "rev-parse", "HEAD"], text=True).strip()
    if revision != contract["tent_revision"]:
        raise ValueError("wrong TENT revision")
    subprocess.run(
        [
            *git,
            "diff",
            "--quiet",
            revision,
            "--",
            TENT,
            "mooncake-transfer-engine/src/transfer_engine.cpp",
            "mooncake-common/src/rpc_client_io_context.cpp",
        ],
        check=True,
    )
    for check in (
        check_links,
        check_c_api,
        check_tests,
        check_rpcs,
        check_capabilities,
        check_config,
        check_metrics,
    ):
        check(root / TENT)
    print(
        "Frozen C declarations, test source/suite coverage, config, environment, directions, metrics and RPC IDs matched"
    )
    with tempfile.TemporaryDirectory() as tmp:
        binary = Path(tmp) / "abi"
        subprocess.run(
            [
                *shlex.split(os.environ.get("CC", "cc")),
                "-std=c11",
                "-Wall",
                "-Werror",
                "-I",
                str(root / TENT / "include"),
                str(HERE / "abi.c"),
                "-o",
                str(binary),
            ],
            check=True,
        )
        if subprocess.check_output([binary]) != (HERE / "abi.lp64").read_bytes():
            raise ValueError("Linux LP64 C or device-plugin layout changed")
    print("C and device-plugin sizes, alignments, field offsets and widths matched")


if __name__ == "__main__":
    main()
