# Installation

## Prerequisites

- Python 3.11 or later
- [uv](https://docs.astral.sh/uv/) installed

## Install the CLI

Use uv to install clair as a global CLI tool:

```bash
uv tool install rivage-clair
```

This installs the `clair` command globally. uv keeps the dependencies of the tool separate from the virtualenv of your project.

Show the version of the CLI:

```bash
clair --version
# clair, version 0.1.1
```

## Add clair to your project

Your Trouve files import from `clair` directly (e.g. `from clair import Trouve`). Add clair as a dependency of your project to get IDE autocompletion and type hints:

```bash
uv add rivage-clair
```

## Upgrade the CLI

```bash
uv tool upgrade rivage-clair
```

## Install from source (development)

Clone the repo. Then use uv to sync the dependencies and to run the CLI:

```bash
git clone https://github.com/rivage-sh/clair.git
cd clair
uv sync
uv run clair --version
```

If the editable install does not work:

```bash
uv pip install --reinstall -e .
```

