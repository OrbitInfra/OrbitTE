"""Package OrbitTE and install an independent public-API consumer outside the checkout."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import shlex
import subprocess
import tarfile
import time
import tomllib


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def build(root, output, toolchain, allow_dirty=False, consumer_kind="host"):
    gpu = consumer_kind == "gpu"
    features = ["rdma", "cuda"] if gpu else ["rdma"]
    binary_name = "orbitte-hardware-peer" if gpu else "orbitte-rdma-host"
    protocol = "orbitte-hardware-peer-v1" if gpu else "orbitte-installed-host-v4"
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
        archives = list((output / 'target/package').glob('orbit-te-*.crate'))
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
        fixture_relative = 'examples/hardware_peer.rs' if gpu else 'tests/hardware/rdma_host.rs'
        fixture = library / fixture_relative
        if fixture.read_bytes() != (root / fixture_relative).read_bytes():
            raise ValueError('packaged consumer differs from the qualified source')
        fixture_modules = library / 'tests/hardware/rdma_host'
        fixture_files = [fixture, *([] if gpu else sorted(fixture_modules.rglob('*.rs')))]
        fixture_hashes = {}
        for path in fixture_files:
            relative = path.relative_to(library)
            if path.read_bytes() != (root / relative).read_bytes():
                raise ValueError('packaged consumer module differs from source')
            fixture_hashes[str(relative)] = digest(path)
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
        fault_library = output / 'liborbitte_cq_fault.so'
        run('fault-library', [*compiler, *flags, '-fPIC', '-shared',
                             str(fault_sources / 'cq_fault.c'), '-ldl', '-o', str(fault_library)])
        for name in ('cq_fault', 'inflight', 'memory', 'dependency', 'accounting'):
            control = output / ('test_' + name)
            run(name + '-control-build', [*compiler, *flags, str(fault_sources / ('test_' + name + '.c')),
                                         '-ldl', '-o', str(control)])
            fault_root = output / (name + '-control')
            fault_root.mkdir()
            environment.update(ORBITTE_RESTART_ROOT=str(fault_root), ORBITTE_RESTART_GENERATION='0')
            run(name + '-control', [str(control)], cwd=output)
        environment.pop('ORBITTE_RESTART_ROOT')
        environment.pop('ORBITTE_RESTART_GENERATION')
        manifest.update(fault_library=str(fault_library), fault_library_sha256=digest(fault_library))
        run('fetch', ['cargo', 'fetch', '--locked'], cwd=library)
        consumer = output / 'consumer'
        (consumer / 'src').mkdir(parents=True)
        shutil.copy2(fixture, consumer / 'src/main.rs')
        if not gpu:
            shutil.copytree(fixture_modules, consumer / 'src/rdma_host')
        (consumer / 'Cargo.toml').write_text(
            '[package]\nname = ' + json.dumps(binary_name) + '\nversion = "0.0.0"\n'
            'edition = "2024"\nrust-version = "1.85"\npublish = false\n'
            '[workspace]\n[dependencies]\n'
            'orbit-te = { path = ' + json.dumps(str(library)) + ', features = ' + json.dumps(features) + ' }\n')
        shutil.copy2(library / 'Cargo.lock', consumer / 'Cargo.lock')
        metadata = json.loads(run('metadata', ['cargo', 'metadata', '--offline', '--format-version', '1'], cwd=consumer))
        dependency = [p for p in metadata['packages'] if p['name'] == 'orbit-te']
        if len(dependency) != 1 or Path(dependency[0]['manifest_path']) != library / 'Cargo.toml':
            raise ValueError('consumer resolved an unexpected OrbitTE source')
        features = next(n['features'] for n in metadata['resolve']['nodes'] if n['id'] == dependency[0]['id'])
        if set(features) != ({'default', 'rdma', 'cuda'} if gpu else {'default', 'rdma'}):
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
        if 'not found' in linkage or 'libibverbs' not in linkage or 'libcuda' in linkage:
            raise ValueError('installed consumer has unexpected native linkage')
        libraries = {word: digest(Path(word)) for word in linkage.split() if word.startswith('/') and Path(word).is_file()}
        manifest.update(status='passed', native_libraries=libraries, archive=str(archive), archive_sha256=digest(archive),
                        library_manifest=str(library / 'Cargo.toml'), features=features,
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
    parser.add_argument('--consumer', choices=('host', 'gpu'), default='host')
    arguments = parser.parse_args()
    build(Path(__file__).resolve().parents[2], arguments.output, arguments.toolchain, arguments.allow_dirty, arguments.consumer)
