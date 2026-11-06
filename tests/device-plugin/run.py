#!/usr/bin/env python3
"""Compile real native plugins and exercise the installed C ABI (CPU only)."""
import argparse
import importlib.util
import socket
from threading import Thread
import os
from pathlib import Path
import subprocess
import tempfile

ROOT=Path(__file__).resolve().parents[2]
HERE=Path(__file__).resolve().parent

def held(exe, library, tmp, env):
    spec=importlib.util.spec_from_file_location('c_controls',ROOT/'tests/c-api/run.py')
    c=importlib.util.module_from_spec(spec);spec.loader.exec_module(c)
    entered,signal=os.pipe();resume,release=os.pipe()
    trace=tmp/'held-trace';trace.unlink(missing_ok=True)
    try:
        with subprocess.Popen([exe,'103',library],stdin=subprocess.PIPE,stdout=subprocess.PIPE,text=True,
                              pass_fds=(entered,release),env=dict(env,ORBITE_PLUGIN_TRACE=str(trace),ORBITE_TEST_ENTERED=str(entered),ORBITE_TEST_RESUME=str(release))) as child:
            try:
                host,port=child.stdout.readline().strip().rsplit(':',1)
                with socket.socket() as listener:
                    listener.bind(('127.0.0.1',0));listener.listen(4);listener.settimeout(30)
                    effects=[]
                    thread=Thread(target=c.relay,args=(listener,(host,int(port)),effects,(signal,resume)),daemon=True)
                    thread.start()
                    child.communicate(f'127.0.0.1:{listener.getsockname()[1]}\n',timeout=45)
                    thread.join(30);assert child.returncode==0 and effects==['read'],effects
                seen=trace.read_text();assert seen.count('F')==1 and seen.endswith('DU'),seen
            finally:
                if child.poll() is None: child.kill()
    finally:
        for fd in (entered,signal,resume,release):os.close(fd)

def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--prefix',type=Path,required=True)
    p.add_argument('--repeat',type=int,default=1)
    args=p.parse_args();assert 1<=args.repeat<=200
    prefix=args.prefix.resolve()
    cc=os.environ.get('CC','cc')
    flags=[cc,'-std=c11','-Wall','-Wextra','-Werror','-Wno-unused-function','-Wno-misleading-indentation','-I',str(prefix/'include')]
    env=dict(os.environ,MC_TENT_CONF='{"transports/tcp/enable":true,"transports/rdma/enable":false,"transports/nvlink/enable":false,"metrics/enabled":false}',MOONCAKE_LOCAL_HOSTNAME='127.0.0.1')
    with tempfile.TemporaryDirectory(prefix='orbite-plugin-') as tmp:
        tmp=Path(tmp);exe=tmp/'consumer'
        subprocess.run([*flags,HERE/'consumer.c','-L',prefix/'lib','-Wl,-rpath,'+str(prefix/'lib'),'-ltent','-o',exe],check=True)
        subprocess.run([*flags,ROOT/'tests/reference/inventory/abi.c','-o',tmp/'abi'],check=True)
        assert subprocess.check_output([tmp/'abi'])==(ROOT/'tests/reference/inventory/abi.lp64').read_bytes()
        cases=[0,*range(1,17),18,19,*range(20,32)]
        libraries={}
        for case in cases:
            lib=tmp/f'plugin-{case}.so'
            subprocess.run([*flags,'-shared','-fPIC',f'-DCASE={case}',HERE/'fake.c','-o',lib],check=True);libraries[case]=lib
        for iteration in range(args.repeat):
            for case,lib in libraries.items():
                trace=tmp/'trace';trace.unlink(missing_ok=True)
                subprocess.run([exe,str(case),lib],env=dict(env,ORBITE_PLUGIN_TRACE=str(trace)),check=True,timeout=60)
                seen=trace.read_text()
                if case in (0,13): assert seen.count('F')==2 and seen.count('D')==1 and seen.endswith('DU'),seen
                elif case in (8,): assert seen.count('F')==1 and 'D' not in seen,seen
                elif case in (9,18): assert seen.count('D')==1,seen
                elif case in (10,11): assert 'F' not in seen and 'D' not in seen,seen
                elif case==12: assert seen.count('F')==1 and seen.endswith('DU'),seen
                else: assert 'A' not in seen,seen
                if case in (1,2,3,4,30,31): assert 'R' not in seen,seen
                if case in (5,6,16,*range(20,29)): assert 'C' not in seen,seen
            for case in (100,101):
                trace=tmp/'isolated';trace.unlink(missing_ok=True)
                subprocess.run([exe,str(case),tmp/'does-not-exist.so'],env=dict(env,ORBITE_PLUGIN_TRACE=str(trace)),check=True,timeout=60)
                assert not trace.exists()
            overflow=tmp/'too-many';overflow.mkdir(exist_ok=True)
            for i in range(9): (overflow/f'{i}.so').write_bytes(libraries[0].read_bytes())
            subprocess.run([exe,'102',overflow],env=env,check=True,timeout=60)
            held(exe,libraries[0],tmp,env)
            print(f'plugin controls {iteration+1}/{args.repeat}: passed',flush=True)

if __name__=='__main__':main()
