"""The version of clair.

This module holds the version alone. The package root and
clair.web_ui.catalog both read it, thus no module must import the package
root to get the version.

The version comes from the package metadata, which hatchling reads from
pyproject.toml. Thus one file holds the version.
"""

from __future__ import annotations

from importlib.metadata import version

__version__ = version("rivage-clair")
