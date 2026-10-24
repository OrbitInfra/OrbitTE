# Contributing

OrbitTE is an independent Rust implementation using frozen Mooncake TENT as
the capability and architecture reference. Start with the
[architecture](architecture.md), [parity contract](tent-parity.md) and
[completion plan](completion-plan.md). The completion plan is the only
execution queue.

## Source changes

Create a focused feature/fix/documentation branch and use conventional commit
messages. Keep concrete resource and behavior owners; preserve physical
quiescence, exact grants, generation fences and retained native failures.

```sh
cargo fmt --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
```

Use feature-specific native checks when changing RDMA or CUDA. Select an
external artifact directory before hardware work, freeze the source and
binaries, use explicit device/resource locks and preserve failed controls.
Missing hardware gates remain unqualified. See [qualification](qualification.md).

## Documentation site

README and the website use Orbit's existing geometric ring. The website uses
Mooncake's Sphinx Book Theme with its search, sidebar navigation, page outline,
code-copy controls and light/dark modes. Technical pages live in `docs/` and
are rendered from those maintained Markdown sources.

Use Python 3.11 or 3.12 and install the pinned lightweight documentation stack
into an environment outside the source checkout:

```sh
python3 -m venv /tmp/orbitte-docs-venv
/tmp/orbitte-docs-venv/bin/pip install -r docs/requirements.txt
/tmp/orbitte-docs-venv/bin/sphinx-build -n -W --keep-going \
  docs /tmp/orbitte-docs/html
python3 scripts/check-docs.py /tmp/orbitte-docs/html
python3 -m http.server 8000 --bind 127.0.0.1 --directory /tmp/orbitte-docs/html
```

Open `http://127.0.0.1:8000/`. Generated HTML, browser screenshots, logs and
validation reports stay outside Git. Docs CI runs the same strict build and
local-link check without CUDA, RDMA devices or a Rust native build. The
`orbitte-docs` CI artifact contains the generated website for preview.

The public site is [orbitinfra.github.io/OrbitTE](https://orbitinfra.github.io/OrbitTE/).
Every push to `main` builds and checks the site, uploads the Pages artifact and
deploys it through the `github-pages` environment. Pull requests run the build
and local-link checks and provide the preview artifact. A manual workflow run
on `main` can redeploy the same source. Deployments use GitHub's scoped Pages
and OIDC permissions, with one deployment running at a time.

Add new pages to the appropriate `toctree` in `docs/index.md`. Keep source
claims aligned with the [capability matrix](capabilities.md), and label planned
paths in architecture figures. The theme stays upstream; Orbit-specific
styles are limited to branding and homepage spacing.

## Theme and asset provenance

The presentation reference is the [Mooncake documentation](https://kvcache-ai.github.io/Mooncake/)
and its [documentation configuration](https://github.com/kvcache-ai/Mooncake/blob/main/docs/source/conf.py).
Sphinx Book Theme is installed as an upstream dependency. Project styles
and assets are maintained separately.

The Orbit ring geometry comes from OrbitKV's existing project branding. The
OrbitTE wordmark and architecture SVG are repository-native assets under
`docs/_static/`. Mooncake's logo, adoption claims and measured performance are
not OrbitTE capabilities.
