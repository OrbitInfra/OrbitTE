#!/usr/bin/env python3
"""Installed CUDA device-plugin gate. Requires an explicit GPU and exclusive lock."""
import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import subprocess

def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--artifact',type=Path,required=True)
    p.add_argument('--gpu',required=True)
    p.add_argument('--gpu-lock',type=Path,required=True)
    p.add_argument('--output',type=Path,required=True)
    a=p.parse_args();artifact=a.artifact.resolve()
    manifest=json.loads((artifact/'manifest.json').read_text())
    assert manifest['status']=='built' and manifest['consumer']=='device-plugin'
    binary=artifact/'installed/bin/orbite-device-plugin'
    assert hashlib.sha256(binary.read_bytes()).hexdigest()==manifest['binary_sha256']
    a.output.mkdir(parents=True,exist_ok=False)
    with a.gpu_lock.open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        info=subprocess.check_output(['nvidia-smi','--id='+a.gpu,'--query-gpu=uuid,name,driver_version','--format=csv,noheader'],text=True)
        (a.output/'gpu.txt').write_text(info)
        env=dict(os.environ,CUDA_VISIBLE_DEVICES=a.gpu,MC_TENT_CONF='{"transports/tcp/enable":true,"transports/rdma/enable":false,"transports/nvlink/enable":false,"metrics/enabled":false}',MOONCAKE_LOCAL_HOSTNAME='127.0.0.1')
        with (a.output/'gate.log').open('w') as log:
            subprocess.run([binary,artifact/'installed/lib/libtent.so'],env=env,stdout=log,stderr=subprocess.STDOUT,check=True,timeout=120)
        (a.output/'result.json').write_text(json.dumps({'status':'passed','source_commit':manifest['source_commit'],'binary_sha256':manifest['binary_sha256'],'gpu':a.gpu,'profile':'CUDA device plugin, loopback TCP, guarded H2D/D2H'},indent=2)+'\n')
if __name__=='__main__':main()
