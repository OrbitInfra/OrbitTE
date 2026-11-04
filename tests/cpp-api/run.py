"""Build a C++20 client against the installed native and classic headers and run both TCP directions."""

import argparse
import json
import os
import shlex
from pathlib import Path
import subprocess
import tempfile
from scatter import line, scatter, shutdown

HERE = Path(__file__).resolve().parent


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--prefix", required=True, type=Path)
    parser.add_argument(
        "--repeat", type=int, default=1, choices=range(1, 201), metavar="1..200"
    )
    args = parser.parse_args()
    prefix = args.prefix.resolve()
    with tempfile.TemporaryDirectory() as directory:
        tmp = Path(directory)
        binary = tmp / "cpp-client"
        pc_env = dict(os.environ, PKG_CONFIG_PATH=str(prefix / "lib/pkgconfig"))

        def flags(option):
            return shlex.split(
                subprocess.check_output(
                    ["pkg-config", option, "tent-cpp"], env=pc_env, text=True
                )
            )

        subprocess.run(
            [
                os.environ.get("CXX", "c++"),
                "-DENABLE_MULTI_PROTOCOL",
                "-Wall",
                "-Wextra",
                "-Werror",
                *flags("--cflags"),
                HERE / "consumer.cpp",
                *flags("--libs"),
                "-Wl,-rpath," + str(prefix / "lib"),
                "-pthread",
                "-o",
                binary,
            ],
            check=True,
        )
        subprocess.run(
            [
                os.environ.get("CXX", "c++"),
                "-std=c++20",
                "-DENABLE_MULTI_PROTOCOL",
                "-fsyntax-only",
                "-I",
                prefix / "include",
                HERE / "frozen_contract.cpp",
            ],
            check=True,
        )
        cfg = {
            "transports/rdma/enable": False,
            "transports/nvlink/enable": False,
            "rpc_server_hostname": "127.0.0.1",
            "metrics/enabled": False,
        }
        env = {
            k: v
            for k, v in os.environ.items()
            if not k.startswith(("MC_", "MOONCAKE_", "NCCL_IB_"))
        }
        file = tmp / "constructor.json"
        file.write_text(json.dumps(dict(cfg, rpc_server_hostname="127.0.0.3")))
        subprocess.run(
            [binary, "contracts", file],
            env=dict(env, MC_TENT_CONF=json.dumps(cfg)),
            timeout=45,
            check=True,
        )
        subprocess.run([binary, "policy"], env=env, timeout=45, check=True)
        env["MC_TENT_CONF"] = json.dumps(cfg)
        shutdown(binary, env)
        for _ in range(args.repeat):
            scatter(binary, cfg, env, False)
            scatter(binary, cfg, env, True)
            for src, requester in (("classic", "native"), ("native", "classic")):
                with subprocess.Popen(
                    [binary, "source", src],
                    stdin=subprocess.PIPE,
                    stdout=subprocess.PIPE,
                    text=True,
                    env=env,
                ) as server:
                    try:
                        name = line(server)
                        assert name and server.poll() is None
                        subprocess.run(
                            [binary, requester, name], env=env, timeout=45, check=True
                        )
                        server.communicate("verify\n", timeout=15)
                        assert server.returncode == 0
                    finally:
                        if server.poll() is None:
                            server.kill()
        print(
            f"Installed C++ engines: constructors, v1 requests, progress, waits and {2 * args.repeat} guarded TCP transfers passed"
        )


if __name__ == "__main__":
    main()
