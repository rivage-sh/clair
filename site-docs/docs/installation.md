# Installation

## Prerequisites

- Python 3.11 or later
- [uv](https://docs.astral.sh/uv/) installed

## Install the CLI

Install clair as a global CLI tool using uv:

```bash
uv tool install rivage-clair
```

This installs the `clair` command globally. uv isolates the tool's dependencies so they don't interfere with your project's virtualenv.

Verify the installation:

```bash
clair --version
# clair, version 0.1.1
```

## Add clair to your project

Your Trouve files import from `clair` directly (e.g. `from clair import Trouve`). To get IDE autocompletion and type checking, add clair as a dependency in your project:

```bash
uv add rivage-clair
```

## Upgrade the CLI

```bash
uv tool upgrade rivage-clair
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

