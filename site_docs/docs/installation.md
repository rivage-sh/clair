# Installation

You need Python 3.11 or later, and [uv](https://docs.astral.sh/uv/).

## Install the CLI

```bash
uv tool install rivage-clair
clair --version
```

uv keeps the dependencies of the `clair` command apart from the environment of your
project.

## Add clair to your project

Your Trouve files import from `clair`, for example `from clair import Table`. Add clair to
the project, so that your editor and your type checker read its types:

```bash
uv add rivage-clair
```

## Upgrade

```bash
uv tool upgrade rivage-clair             # the clair command
uv lock --upgrade-package rivage-clair   # your project
uv sync
```

## Install from source

```bash
git clone https://github.com/rivage-sh/clair.git
cd clair
uv sync
uv run clair --version
```

If `uv run clair` cannot import clair after a change, install the project again:

```bash
uv sync --reinstall
```
