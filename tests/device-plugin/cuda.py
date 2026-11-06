#!/usr/bin/env python3
"""Run the installed native CUDA plugin against a CPU Driver API shim."""
import argparse
import os
from pathlib import Path
import subprocess
import tempfile
ROOT=Path(__file__).resolve().parents[2]
def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--prefix',type=Path,required=True);p.add_argument('--repeat',type=int,default=1);a=p.parse_args();assert 1<=a.repeat<=200
    prefix=a.prefix.resolve();cc=os.environ.get('CC','cc')
    with tempfile.TemporaryDirectory(prefix='orbite-plugin-cuda-') as tmp:
        tmp=Path(tmp);exe=tmp/'consumer'
        subprocess.run([cc,'-std=c11','-Wall','-Wextra','-Werror','-I',prefix/'include',ROOT/'tests/hardware/device_plugin.c','-L',prefix/'lib','-Wl,-rpath,'+str(prefix/'lib'),'-ltent','-ldl','-o',exe],check=True)
        env=dict(os.environ,LD_LIBRARY_PATH=str(tmp),MC_TENT_CONF='{"transports/tcp/enable":true,"transports/shm/enable":true,"transports/rdma/enable":false,"transports/nvlink/enable":false,"metrics/enabled":false}',MOONCAKE_LOCAL_HOSTNAME='127.0.0.1')
        shim=[cc,'-std=c11','-D_POSIX_C_SOURCE=200809L','-shared','-fPIC','-pthread',ROOT/'tests/python/cuda_shim.c','-o',tmp/'libcuda.so.1']
        subprocess.run(shim,check=True)
        for i in range(a.repeat):
            for fault in (0,20,21,22,26,3,6,7,8,9):
                subprocess.run([exe,prefix/'lib/libtent.so',str(fault)],env=env,check=True,timeout=60,stdout=subprocess.DEVNULL)
            print(f'CUDA plugin shim {i+1}/{a.repeat}: passed',flush=True)
        subprocess.run([*shim,'-DORBITE_PLUGIN_MISSING_COPY'],check=True)
        subprocess.run([exe,prefix/'lib/libtent.so','99'],env=env,check=True,timeout=60)
if __name__=='__main__':main()
