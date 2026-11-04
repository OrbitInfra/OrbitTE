"""Discover the configuration spellings present in the pinned TENT sources."""

from collections import defaultdict
import re

LITERAL = r'"([^"\n]+)"'


def sources(tent):
    return sorted(
        p
        for root in ("include/tent", "src")
        for p in (tent / root).rglob("*")
        if p.suffix in {".cpp", ".h", ".cc", ".hpp"} and "thirdparty" not in p.relative_to(tent).parts
    )


def calls(text, pattern):
    """One balanced C++ call at a time, retaining nested default expressions."""
    for m in re.finditer(pattern, text):
        start = m.end()
        depth = 1
        args = []
        quoted = False
        for i in range(start, len(text)):
            c = text[i]
            if c == '"' and text[i - 1] != "\\":
                quoted = not quoted
            if quoted:
                continue
            if c in "([{":
                depth += 1
            elif c in ")]}":
                depth -= 1
            if depth == 0 or (depth == 1 and c == ","):
                args.append(" ".join(text[start:i].split()))
                if not depth:
                    yield m, args
                    break
                start = i + 1


def config_surface(tent):
    keys, env = defaultdict(list), defaultdict(list)
    for path in sources(tent):
        name = str(path.relative_to(tent))
        # Keep original line numbers for links into the frozen revision.
        text = re.sub(
            r'"(?:\\.|[^"\\])*"|/\*.*?\*/|//[^\n]*',
            lambda m: m[0] if m[0].startswith('"') else "\n" * m[0].count("\n"),
            path.read_text(),
            flags=re.S,
        )

        def record(table, key, match, value):
            table[key].append((name, text.count("\n", 0, match.start()) + 1, value))

        for m, args in calls(
            text,
            r"\b(?:conf|config|runtime_config)_?\s*(?:->|\.)\s*(?:get(?:Array)?(?:<[^>]+>)?|dumpSubtree)\s*\(",
        ):
            if args and (quoted := re.fullmatch(LITERAL, args[0])):
                record(keys, quoted[1], m, args[1] if len(args) > 1 else "")
        for m in re.finditer(
            r"\bSET_(DEVICE|ENDPOINT|WORKERS)\(\s*(\w+)\s*,\s*params->([\w.]+)\)", text
        ):
            record(keys, f"transports/rdma/{m[1].lower()}/{m[2]}", m, m[3])
        for m in re.finditer(
            r"\bset(Config|BoolConfig|ArrayConfig)\(config,\s*" + LITERAL + r",\s*" + LITERAL, text
        ):
            record(keys, m[3], m, "legacy mapping")
            record(env, m[2], m, m[3])
        for m in re.finditer(r"\bgetenv\(\s*" + LITERAL, text):
            record(env, m[1], m, "direct")
        for m in re.finditer(r"\b(?:ENV_)?METRICS_[A-Z_]+\s*=\s*" + LITERAL, text):
            record(env if m[0].startswith("ENV_") else keys, m[1], m, "metrics loader")
        for m in re.finditer(r'"((?:MC|MPCOMM|ASCEND|HCCL|MF|FI|NCCL|MOONCAKE)_[A-Z0-9_]+)"', text):
            record(env, m[1], m, "native spelling")
        if name == "src/runtime/transfer_engine_impl.cpp":
            for m in re.finditer(r"constexpr.*?kKey\s*=\s*" + LITERAL, text):
                record(keys, m[1], m, "checked parser")
        if name == "src/runtime/transport_selector.cpp":
            for m in re.finditer(r"policy_json\.(?:contains|value)\(\s*" + LITERAL, text):
                record(keys, "policy[]/" + m[1], m, "selector field")
        if name == "src/transport/rdma/rdma_transport.cpp":
            for m in re.finditer(r"pool_json\.(?:contains|value)\(\s*" + LITERAL, text):
                record(keys, "transports/rdma/endpoint/qp_pools[]/" + m[1], m, "pool field")
        if name == "src/runtime/hp_tcp_transport_config.cpp":
            for m in re.finditer(r"Read(?:StringList|String|Unsigned)\(hp_tcp,\s*" + LITERAL, text):
                record(keys, "transports/hp_tcp/" + m[1], m, "HP-TCP parameters")
        # Literal schema paths also occur in preserved overrides, ConfigHelper,
        # registration macros, lifecycle-only declarations and plugin fallbacks.
        for m in re.finditer(
            r'"((?:transports|runtime_queue|topology|notification|metrics|receiver_credit|qos)/[a-z][a-z0-9_/]*)"',
            text,
        ):
            if not m[1].endswith("/"):
                record(keys, m[1], m, "schema path")
    # CoroRpcAgent uses the shared TE client pool through the parent transfer
    # engine header. The sibling Store pool is outside the TENT client path.
    common = "../../mooncake-common/src/rpc_client_io_context.cpp"
    text = (tent / common).read_text()
    for match in re.finditer(r"Variables::(MC_(?:TE_)?RPC_CLIENT_IO_THREADS)", text):
        env[match[1]].append((common, text.count("\n", 0, match.start()) + 1, "RPC client pool"))
    return keys, env


def lifecycle(tent):
    code = (tent / "src/common/config_lifecycle.cpp").read_text()
    fields = dict(re.findall(r'\{"([^"]+)", ConfigFieldMatch::k(Exact|Subtree)\}', code))
    engine = (tent / "src/runtime/transfer_engine_impl.cpp").read_text()
    preserved = set(re.findall(r'restoreExplicitConfigValue\(config,\s*"([^"]+)"', engine))

    def classify(key):
        path = key.replace("[]", "")
        candidate = path in fields or any(
            kind == "Subtree" and path.startswith(prefix + "/") for prefix, kind in fields.items()
        )
        precedence = (
            "restore explicit value"
            if path in preserved
            else "restore true by forcing TCP"
            if path == "transports/force_tcp"
            else "metrics config over metrics environment"
            if path.startswith("metrics/")
            else "merged config"
        )
        return ("runtime candidate" if candidate else "bootstrap", precedence)

    return classify
