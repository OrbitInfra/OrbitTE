"""Package OrbitTE and install an independent RDMA consumer outside the checkout."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tarfile
import time
import tomllib


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def build(root, output, toolchain, allow_dirty=False):
    root, output = root.resolve(), output.resolve()
    if output.is_relative_to(root):
        raise ValueError('installed artifacts must stay outside the source checkout')
    output.mkdir(parents=True, exist_ok=False)
    environment = {**os.environ, 'RUSTUP_TOOLCHAIN': toolchain,
                   'CARGO_TARGET_DIR': str(output / 'target')}
    manifest = {'status': 'running', 'toolchain': toolchain, 'started_unix': time.time(),
                'commands': [], 'performance_measurement': False,
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
        package = ['cargo', 'package', '--locked', '--no-verify', '--features', 'rdma']
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
        fixture = library / 'tests/hardware/rdma_host.rs'
        if fixture.read_bytes() != (root / 'tests/hardware/rdma_host.rs').read_bytes():
            raise ValueError('packaged consumer differs from the qualified source')
        fixture_modules = library / 'tests/hardware/rdma_host'
        fixture_files = [fixture, *sorted(fixture_modules.rglob('*.rs'))]
        fixture_hashes = {}
        for path in fixture_files:
            relative = path.relative_to(library)
            if path.read_bytes() != (root / relative).read_bytes():
                raise ValueError('packaged consumer module differs from source')
            fixture_hashes[str(relative)] = digest(path)
        run('fetch', ['cargo', 'fetch', '--locked'], cwd=library)
        consumer = output / 'consumer'
        (consumer / 'src').mkdir(parents=True)
        shutil.copy2(fixture, consumer / 'src/main.rs')
        shutil.copytree(fixture_modules, consumer / 'src/rdma_host')
        (consumer / 'Cargo.toml').write_text(
            '[package]\nname = "orbitte-rdma-host"\nversion = "0.0.0"\n'
            'edition = "2024"\nrust-version = "1.85"\npublish = false\n'
            '[workspace]\n[dependencies]\n'
            'orbit-te = { path = ' + json.dumps(str(library)) + ', features = ["rdma"] }\n')
        shutil.copy2(library / 'Cargo.lock', consumer / 'Cargo.lock')
        metadata = json.loads(run('metadata', ['cargo', 'metadata', '--offline', '--format-version', '1'], cwd=consumer))
        dependency = [p for p in metadata['packages'] if p['name'] == 'orbit-te']
        if len(dependency) != 1 or Path(dependency[0]['manifest_path']) != library / 'Cargo.toml':
            raise ValueError('consumer resolved an unexpected OrbitTE source')
        features = next(n['features'] for n in metadata['resolve']['nodes'] if n['id'] == dependency[0]['id'])
        if set(features) != {'default', 'rdma'}:
            raise ValueError('host consumer must use only the normal RDMA dependency')
        old = tomllib.loads((library / 'Cargo.lock').read_text())['package']
        new = tomllib.loads((consumer / 'Cargo.lock').read_text())['package']
        pinned = {(p['name'], p['version'], p.get('source'), p.get('checksum')) for p in old}
        if any((p['name'], p['version'], p.get('source'), p.get('checksum')) not in pinned
               for p in new if p['name'] != 'orbitte-rdma-host'):
            raise ValueError('installed dependency graph changed the packaged lockfile')
        run('format', ['cargo', 'fmt', '--check'], cwd=consumer)
        run('clippy', ['cargo', 'clippy', '--locked', '--offline', '--all-targets', '--', '-D', 'warnings'], cwd=consumer)
        run('test-relay', ['cargo', 'test', '--locked', '--offline'], cwd=consumer)
        prefix = output / 'installed'
        run('install', ['cargo', 'install', '--path', str(consumer), '--root', str(prefix),
                        '--locked', '--offline'], cwd=consumer)
        binary = prefix / 'bin/orbitte-rdma-host'
        probe = run('protocol', [str(binary), '--protocol'], cwd=output).strip()
        if probe != 'orbitte-installed-host-v3':
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
    arguments = parser.parse_args()
    build(Path(__file__).resolve().parents[2], arguments.output, arguments.toolchain, arguments.allow_dirty)
