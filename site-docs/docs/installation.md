# Installation

## Prerequisites

- Python 3.11 or later
- [uv](https://docs.astral.sh/uv/) installed

## Install clair

Install clair as a global CLI tool using uv:

```bash
uv tool install git+https://github.com/rivage-sh/clair.git
```

This installs the `clair` command globally. uv isolates the tool's dependencies so they don't interfere with your project's virtualenv.

Verify the installation:

```bash
clair --version
# clair, version 0.1.0
```

## Upgrade

```bash
uv tool upgrade clair
```

## Install from source (development)

Clone the repo, then sync dependencies and run via uv:

```bash
git clone https://github.com/rivage-sh/clair.git
cd clair
uv sync
uv run clair --version
```

If the editable install seems broken:

```bash
uv pip install --reinstall -e .
```

## Note: not yet on PyPI

clair is not yet available on PyPI. Install directly from GitHub as shown above.
