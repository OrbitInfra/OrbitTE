"""Written, unrun P10 GDS gate: real NVMe/GPU IO with held or permanently missing receipts."""
import json
import os
from pathlib import Path
import subprocess
from gds_run import counters
from storage_cuda_run import installed_case, options, sha


def main():
    parser = options(__doc__)
    parser.add_argument("--mode", required=True, choices=("drain", "quarantine"))
    args = parser.parse_args()
    with installed_case(args, "gds-p10", "native-gds-p10", Path(__file__)) as (command, evidence):
        manifest = json.loads((args.artifact / "manifest.json").read_text())
        observer = args.artifact / "liborbite_gds_p10.so"
        if sha(observer) != manifest["observer_sha256"]:
            raise ValueError("observer differs from compiled artifact")
        control = args.output / "control"
        control.mkdir()
        config = args.output / "cufile.json"
        config.write_text(json.dumps({"properties": {"allow_compat_mode": False}}))
        env = dict(os.environ, CUDA_VISIBLE_DEVICES=args.gpu, CUFILE_ENV_PATH_JSON=str(config.resolve()),
                   LD_PRELOAD=str(observer.resolve()), ORBITE_GDS_P10_CONTROL=str(control.resolve()), ORBITE_GDS_P10_MODE=args.mode)
        env.pop("CUFILE_FORCE_COMPAT_MODE", None)
        stats = Path("/proc/driver/nvidia-fs/stats")
        before = stats.read_text()
        (args.output / "nvidia-fs.before").write_text(before)
        with (args.output / "stdout").open("w") as out, (args.output / "stderr").open("w") as err:
            result = subprocess.run(command, env=env, stdin=subprocess.DEVNULL, stdout=out, stderr=err, timeout=150)
        after = stats.read_text()
        (args.output / "nvidia-fs.after").write_text(after)
        first, last = counters(before), counters(after)
        delta = {k: last["Reads"][k]-first["Reads"][k] for k in ("ok", "err", "readMiB")}
        evidence.update(mode=args.mode, nvidia_fs_read_delta=delta, observer_sha256=sha(observer))
        if result.returncode or (args.output / "stdout").read_text().strip() != "P10_GDS_OK mode="+args.mode:
            raise RuntimeError("installed GDS owner control failed")
        if delta["readMiB"] != 2 or delta["ok"] < 1 or delta["err"] != 0:
            raise RuntimeError("native-route evidence does not match the single READ")


if __name__ == "__main__":
    main()
