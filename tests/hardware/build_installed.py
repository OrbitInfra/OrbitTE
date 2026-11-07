"""Package OrbiTE and install an independent public-API consumer outside the checkout."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import shlex
import subprocess
import sys
import tarfile
import time
import tomllib


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def build(root, output, toolchain, allow_dirty=False, consumer_kind="host"):
    gpu = consumer_kind == "gpu"
    nvlink = consumer_kind == "nvlink"
    storage = consumer_kind in ("storage-cuda", "gds", "gds-p10")
    features, binary_name, protocol, fixture_relative = {
        "fabric": (["fabric"], "orbite-fabric", "orbite-installed-fabric", "tests/hardware/fabric.rs"),
        "python-cuda": (["cuda"], "orbite-python-cuda", "orbite-installed-python-cuda", "tests/hardware/python_cuda.py"),
        "peer-retirement": (["rdma", "cuda"], "orbite-peer-retirement", "orbite-installed-peer-retirement", "tests/hardware/peer_retirement.rs"),
        "local-copy": (["cuda"], "orbite-local-copy", "orbite-installed-local-copy", "tests/hardware/local_copy.rs"),
        "peer-gpu-source": (["cuda"], "orbite-peer-gpu-source", "orbite-peer-gpu-source", "tests/hardware/peer_gpu_source.rs"),
        "peer-gpu-requester": ([], "orbite-peer-gpu-requester", "orbite-peer-gpu-requester", "tests/hardware/peer_gpu_requester.rs"),
        "shm": ([], "orbite-shm", "orbite-installed-shm", "tests/hardware/shm.rs"),
        "hp-tcp": ([], "orbite-hp-tcp", "orbite-installed-hp-tcp", "tests/hardware/hp_tcp.rs"),
        "host": (["rdma"], "orbite-rdma-host", "orbite-installed-host-v4", "tests/hardware/rdma_host.rs"),
        "gpu": (["rdma", "cuda"], "orbite-hardware-peer", "orbite-hardware-peer-v1", "examples/hardware_peer.rs"),
        "mnnvl": (["cuda"], "orbite-mnnvl", "orbite-installed-mnnvl", "tests/hardware/mnnvl.rs"),
        "nvlink": (["cuda"], "orbite-nvlink", "orbite-installed-nvlink-v1", "tests/hardware/nvlink.rs"),
        "gds-p10": (["gds"], "orbite-gds-p10", "orbite-installed-gds-p10", "tests/hardware/gds_p10.rs"),
        "gds": (["gds"], "orbite-gds", "orbite-installed-gds-v1", "tests/hardware/gds.rs"),
        "mixed-gpu": (["rdma", "cuda"], "orbite-mixed-gpu", "orbite-installed-mixed-gpu", "tests/hardware/mixed_gpu.rs"),
        "staging-control": (["rdma", "cuda"], "orbite-staging-control", "orbite-installed-staging-control", "tests/hardware/staging_control.rs"),
        "storage-cuda": (["cuda", "io-uring"], "orbite-storage-cuda", "orbite-installed-storage-cuda-v1", "tests/hardware/storage_cuda.rs"),
    }[consumer_kind]
    root, output = root.resolve(), output.resolve()
    if output.is_relative_to(root):
        raise ValueError('installed artifacts must stay outside the source checkout')
    output.mkdir(parents=True, exist_ok=False)
    environment = {**os.environ, 'RUSTUP_TOOLCHAIN': toolchain,
                   'CARGO_TARGET_DIR': str(output / 'target')}
    manifest = {'status': 'running', 'toolchain': toolchain, 'started_unix': time.time(),
                'commands': [], 'performance_measurement': False, 'consumer': consumer_kind,
                'compiler_environment': {key: environment.get(key) for key in ('CC', 'CXX', 'CLANG_PATH', 'LIBCLANG_PATH')}}

    def run(name, arguments, cwd=root):
        result = subprocess.run(arguments, cwd=cwd, env=environment, text=True,
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=600)
        stdout, stderr = output / (name + '.stdout'), output / (name + '.stderr')
        stdout.write_text(result.stdout)
        stderr.write_text(result.stderr)
        manifest['commands'].append({'name': name, 'arguments': arguments, 'cwd': str(cwd),
                                     'returncode': result.returncode,
                                     'stdout_sha256': digest(stdout), 'stderr_sha256': digest(stderr)})
        if result.returncode:
            raise RuntimeError(f'{name} failed; inspect {stderr}: {result.stderr[-3000:]}')
        return result.stdout

    try:
        dirty = run('git-status', ['git', 'status', '--porcelain'])
        if dirty and not allow_dirty:
            raise ValueError('commit the qualification inputs first, or explicitly use --allow-dirty for development')
        manifest.update(source_commit=run('git-head', ['git', 'rev-parse', 'HEAD']).strip(),
                        dirty=bool(dirty), rustc=run('rustc', ['rustc', '-vV']),
                        cargo=run('cargo-version', ['cargo', '-V']))
        package = ['cargo', 'package', '--locked', '--no-verify', '--features', ','.join(features)]
        if allow_dirty:
            package.append('--allow-dirty')
        run('package', package)
        archives = list((output / 'target/package').glob('orbite-*.crate'))
        if len(archives) != 1:
            raise ValueError('expected one packaged crate')
        archive = output / archives[0].name
        shutil.copy2(archives[0], archive)
        unpack = output / 'crate'
        unpack.mkdir()
        with tarfile.open(archive) as bundle:
            bundle.extractall(unpack, filter='data')
        packages = [path for path in unpack.iterdir() if path.is_dir()]
        if len(packages) != 1:
            raise ValueError('expected one package root')
        library = packages[0]
        fixture = library / fixture_relative
        if consumer_kind == 'python-cuda':
            patch = 'patch.crates-io.orbite.path=' + json.dumps(str(root))
            run('python-package', [*package, '-p', 'orbite-python', '--config', patch])
            ffi_archive, = (output / 'target/package').glob('orbite-python-*.crate')
            with tarfile.open(ffi_archive) as bundle:
                bundle.extractall(unpack, filter='data')
            ffi, = unpack.glob('orbite-python-*')
            project = output / 'python-project'
            project.mkdir()
            shutil.copytree(library / 'python', project / 'python')
            shutil.copy2(library / 'docs/python.md', project / 'README.md')
            shutil.copy2(library / 'LICENSE', project / 'LICENSE')
            config = (library / 'pyproject.toml').read_text().replace(
                '"crates/orbite-python/Cargo.toml"', json.dumps(str(ffi / 'Cargo.toml'))).replace(
                '"docs/python.md"', '"README.md"')
            (project / 'pyproject.toml').write_text(config)
            # Maturin's metadata pass does not forward --config on every
            # supported release; use Cargo's immutable per-project config.
            (project / '.cargo').mkdir()
            (project / '.cargo/config.toml').write_text(
                '[patch.crates-io]\norbite = { path = ' + json.dumps(str(library)) + ' }\n')
            wheels = output / 'wheels'
            run('wheel', [sys.executable, '-m', 'maturin', 'build', '--locked', '--release',
                          '--interpreter', sys.executable, '--features', 'pyo3/extension-module,cuda',
                          '--out', str(wheels), '--config',
                          'patch.crates-io.orbite.path=' + json.dumps(str(library))], cwd=project)
            wheel, = wheels.glob('*.whl')
            fixtures = {}
            for name in ('python_cuda.py', 'python_cuda_run.py'):
                src = library / 'tests/hardware' / name
                if src.read_bytes() != (root / 'tests/hardware' / name).read_bytes():
                    raise ValueError('packaged Python gate differs from source')
                shutil.copy2(src, output / name)
                fixtures[name] = digest(output / name)
            manifest.update(status='built', protocol=protocol, package_sha256=digest(archive),
                            python_package_sha256=digest(ffi_archive), wheel=str(wheel),
                            wheel_sha256=digest(wheel), fixture_files_sha256=fixtures)
            return
        if fixture.read_bytes() != (root / fixture_relative).read_bytes():
            raise ValueError('packaged consumer differs from the qualified source')
        fixture_modules = library / 'tests/hardware/rdma_host'
        fixture_files = [fixture, *(sorted(fixture_modules.rglob('*.rs')) if consumer_kind == 'host' else [])]
        if consumer_kind == 'mixed-gpu':
            fixture_files.extend(library / 'tests/hardware' / name
                                 for name in ('mixed_gpu.py', 'run.py', 'supervise.py'))
        if consumer_kind == 'fabric':
            fixture_files.append(library / 'tests/hardware/fabric_run.py')
        fixture_hashes = {}
        for path in fixture_files:
            relative = path.relative_to(library)
            if path.read_bytes() != (root / relative).read_bytes():
                raise ValueError('packaged consumer module differs from source')
            fixture_hashes[str(relative)] = digest(path)
        if storage or consumer_kind in ('peer-retirement', 'local-copy', 'hp-tcp', 'shm', 'mnnvl', 'peer-gpu-source', 'peer-gpu-requester', 'staging-control'):
            drivers = [{'peer-retirement': 'peer_retirement_run.py', 'local-copy': 'local_copy_run.py', 'staging-control': 'staging_control_run.py', 'peer-gpu-source': 'peer_gpu_tcp_run.py', 'peer-gpu-requester': 'peer_gpu_tcp_run.py', 'hp-tcp': 'hp_tcp_run.py', 'shm': 'shm_run.py', 'mnnvl': 'mnnvl_run.py'}.get(consumer_kind, 'storage_cuda_run.py')]
            if consumer_kind in ('gds', 'gds-p10'):
                drivers.append('gds_run.py')
            if consumer_kind == 'gds-p10':
                drivers += ['gds_p10_run.py', 'gds_p10_observer.c']
            for name in drivers:
                relative = 'tests/hardware/' + name
                driver = library / relative
                if driver.read_bytes() != (root / relative).read_bytes():
                    raise ValueError('packaged storage driver differs from source')
                fixture_hashes[relative] = digest(driver)
                shutil.copy2(driver, output / driver.name)
            if consumer_kind == 'gds-p10':
                observer = output / 'liborbite_gds_p10.so'
                compiler = shlex.split(environment.get('CC', 'cc'))
                run('observer-build', [*compiler, '-std=c11', '-O2', '-Wall', '-Wextra', '-Werror', '-pthread',
                                      '-fPIC', '-shared', str(output / 'gds_p10_observer.c'), '-ldl', '-o', str(observer)])
                manifest.update(observer=str(observer), observer_sha256=digest(observer))
        elif nvlink:
            for relative in ('tests/hardware/nvlink_observer.c', 'tests/hardware/nvlink_run.py'):
                source = library / relative
                if source.read_bytes() != (root / relative).read_bytes():
                    raise ValueError('packaged NVLink fixture differs from source')
                fixture_hashes[relative] = digest(source)
                shutil.copy2(source, output / source.name)
            compiler = shlex.split(environment.get('CC', 'cc'))
            manifest['observer_compiler'] = run('observer-compiler', [*compiler, '--version'])
            observer = output / 'libnvlink_observer.so'
            run('observer-build', [*compiler, '-std=c11', '-O2', '-Wall', '-Wextra', '-Werror', '-pthread',
                                   '-fPIC', '-shared', str(output / 'nvlink_observer.c'), '-ldl', '-o', str(observer)])
            manifest.update(observer=str(observer), observer_sha256=digest(observer))
        elif consumer_kind in ('host', 'gpu'):
            inventory_tool = output / 'discovery_inventory.py'
            inventory_source = library / 'tests/hardware/discovery_inventory.py'
            if inventory_source.read_bytes() != (root / 'tests/hardware/discovery_inventory.py').read_bytes():
                raise ValueError('packaged discovery inventory differs from source')
            shutil.copy2(inventory_source, inventory_tool)
            manifest.update(inventory_tool=str(inventory_tool), inventory_tool_sha256=digest(inventory_tool))
            fixture_hashes['tests/hardware/discovery_inventory.py'] = digest(inventory_tool)
            fault_sources = library / 'tests/hardware/cq_fault'
            for path in sorted(fault_sources.glob('*.c')):
                relative = path.relative_to(library)
                if path.read_bytes() != (root / relative).read_bytes():
                    raise ValueError('packaged CQ fault tool differs from source')
                fixture_hashes[str(relative)] = digest(path)
            compiler = shlex.split(environment.get('CC', 'cc'))
            manifest['fault_compiler'] = run('fault-compiler', [*compiler, '--version'])
            flags = ['-std=c11', '-O2', '-Wall', '-Wextra', '-Werror', '-pthread']
            fault_library = output / 'liborbite_cq_fault.so'
            run('fault-library', [*compiler, *flags, '-fPIC', '-shared',
                                 str(fault_sources / 'cq_fault.c'), '-ldl', '-o', str(fault_library)])
            for name in ('cq_fault', 'inflight', 'memory', 'dependency', 'accounting'):
                control = output / ('test_' + name)
                run(name + '-control-build', [*compiler, *flags, str(fault_sources / ('test_' + name + '.c')),
                                             '-ldl', '-o', str(control)])
                fault_root = output / (name + '-control')
                fault_root.mkdir()
                environment.update(ORBITE_RESTART_ROOT=str(fault_root), ORBITE_RESTART_GENERATION='0')
                run(name + '-control', [str(control)], cwd=output)
            environment.pop('ORBITE_RESTART_ROOT')
            environment.pop('ORBITE_RESTART_GENERATION')
            manifest.update(fault_library=str(fault_library), fault_library_sha256=digest(fault_library))
        if gpu:
            source = library / 'tests/hardware/nvlink_observer.c'
            fixture_hashes['tests/hardware/nvlink_observer.c'] = digest(source)
            if source.read_bytes() != (root / 'tests/hardware/nvlink_observer.c').read_bytes():
                raise ValueError('packaged CUDA observer differs from source')
            observer = output / 'libcuda_staging_observer.so'
            run('staging-observer-build', [*compiler, *flags, '-fPIC', '-shared', str(source), '-ldl', '-o', str(observer)])
            manifest.update(cuda_observer=str(observer), cuda_observer_sha256=digest(observer))
        run('fetch', ['cargo', 'fetch', '--locked'], cwd=library)
        consumer = output / 'consumer'
        (consumer / 'src').mkdir(parents=True)
        shutil.copy2(fixture, consumer / 'src/main.rs')
        if consumer_kind == 'host':
            shutil.copytree(fixture_modules, consumer / 'src/rdma_host')
        (consumer / 'Cargo.toml').write_text(
            '[package]\nname = ' + json.dumps(binary_name) + '\nversion = "0.0.0"\n'
            'edition = "2024"\nrust-version = "1.85"\npublish = false\n'
            '[workspace]\n[dependencies]\n'
            'orbite = { path = ' + json.dumps(str(library)) + ', features = ' + json.dumps(features) + ' }\n'
            + ('cudarc = { version = "0.19.0", default-features = false, features = ["std", "driver", "dynamic-loading", "cuda-13000"] }\nserde_json = "1.0.151"\n' if consumer_kind == 'local-copy' else '')
            + ('libloading = "0.8.9"\n' if consumer_kind == 'storage-cuda' else ''))
        shutil.copy2(library / 'Cargo.lock', consumer / 'Cargo.lock')
        metadata = json.loads(run('metadata', ['cargo', 'metadata', '--offline', '--format-version', '1'], cwd=consumer))
        dependency = [p for p in metadata['packages'] if p['name'] == 'orbite']
        if len(dependency) != 1 or Path(dependency[0]['manifest_path']) != library / 'Cargo.toml':
            raise ValueError('consumer resolved an unexpected OrbiTE source')
        resolved = next(n['features'] for n in metadata['resolve']['nodes'] if n['id'] == dependency[0]['id'])
        expected_features = {'default', *features}
        if 'gds' in features:
            expected_features.add('cuda')
        if set(resolved) != expected_features:
            raise ValueError('consumer must use only its declared normal dependency features')
        old = tomllib.loads((library / 'Cargo.lock').read_text())['package']
        new = tomllib.loads((consumer / 'Cargo.lock').read_text())['package']
        pinned = {(p['name'], p['version'], p.get('source'), p.get('checksum')) for p in old}
        if any((p['name'], p['version'], p.get('source'), p.get('checksum')) not in pinned
               for p in new if p['name'] != binary_name):
            raise ValueError('installed dependency graph changed the packaged lockfile')
        run('format', ['cargo', 'fmt', '--check'], cwd=consumer)
        run('clippy', ['cargo', 'clippy', '--locked', '--offline', '--all-targets', '--', '-D', 'warnings'], cwd=consumer)
        run('test-relay', ['cargo', 'test', '--locked', '--offline'], cwd=consumer)
        prefix = output / 'installed'
        run('install', ['cargo', 'install', '--path', str(consumer), '--root', str(prefix),
                        '--locked', '--offline'], cwd=consumer)
        binary = prefix / 'bin' / binary_name
        probe = run('protocol', [str(binary), '--protocol'], cwd=output).strip()
        if probe != protocol:
            raise ValueError('unexpected installed consumer protocol')
        linkage = run('ldd', ['ldd', str(binary)], cwd=output)
        if 'libfabric' in linkage or 'not found' in linkage or ('libibverbs' in linkage) != ('rdma' in features) or 'libcuda' in linkage or (storage and ('liburing' in linkage or 'libcufile' in linkage)):
            raise ValueError('installed consumer has unexpected native linkage')
        libraries = {word: digest(Path(word)) for word in linkage.split() if word.startswith('/') and Path(word).is_file()}
        manifest.update(status='passed', native_libraries=libraries, archive=str(archive), archive_sha256=digest(archive),
                        library_manifest=str(library / 'Cargo.toml'), features=resolved,
                        fixture_sha256=digest(fixture), fixture_files_sha256=fixture_hashes, consumer_lock_sha256=digest(consumer / 'Cargo.lock'),
                        binary=str(binary), binary_sha256=digest(binary), protocol=probe)
    except Exception as error:
        manifest.update(status='failed', error=str(error))
        raise
    finally:
        manifest['finished_unix'] = time.time()
        (output / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
    print(json.dumps(manifest, indent=2))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', required=True, type=Path)
    parser.add_argument('--toolchain', default='stable')
    parser.add_argument('--allow-dirty', action='store_true')
    parser.add_argument('--consumer', choices=('fabric', 'python-cuda', 'peer-retirement', 'host', 'gpu', 'nvlink', 'storage-cuda', 'gds', 'gds-p10', 'hp-tcp', 'shm', 'mnnvl', 'peer-gpu-source', 'peer-gpu-requester', 'staging-control', 'mixed-gpu', 'local-copy'), default='host')
    arguments = parser.parse_args()
    build(Path(__file__).resolve().parents[2], arguments.output, arguments.toolchain, arguments.allow_dirty, arguments.consumer)
