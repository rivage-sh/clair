"""Load the project routing file, ``__routing__.py``.

Routing lives in the project, not in ``~/.clair/environments.yml``:

* Routing is not a secret. A team commits it and reviews it like other code.
* Routing gains the most from Python. A rule reads an environment variable, so
  one committed rule gives each developer a separate target.
* A project-local file matches the clair version that the project pins.

The file defines a ``routing`` dict. Each key is an environment name. That name
is the join key: it matches a top-level key in ``~/.clair/environments.yml``.
Each value is a ``RoutingConfig``, a callable, or None for passthrough.
"""

from __future__ import annotations

import hashlib
import importlib.util
import sys
from pathlib import Path
from typing import NamedTuple

from clair.environments.routing import RoutingConfig, RoutingRule
from clair.exceptions import InvalidRoutingFileError

ROUTING_FILE_NAME = "__routing__.py"
ROUTING_TABLE_ATTRIBUTE = "routing"

# Cache key is (resolved path, modification time), so an edit reloads the file
# but repeated loads in one process do not run the file again. A rule that reads
# a keychain or a secret store must not run two times.
_routing_table_cache: dict[tuple[str, int], dict[str, RoutingRule | None]] = {}


class ProjectRouting(NamedTuple):
    """The outcome of a routing file lookup for one environment."""

    rule: RoutingRule | None
    file_path: Path | None
    environment_names: list[str]
    has_entry: bool = False

    @property
    def file_exists(self) -> bool:
        """Tell the caller if the project has a ``__routing__.py``."""
        return self.file_path is not None

    @property
    def is_unnamed_environment(self) -> bool:
        """Tell the caller if the file omits this environment.

        An explicit ``"prod": None`` entry is a decision, so it reads as named.
        A missing key is almost always a typo, so it reads as unnamed.
        """
        return self.file_exists and not self.has_entry


def _module_name_for(path: Path) -> str:
    """Build a module name that is unique per routing file path."""
    digest = hashlib.md5(str(path).encode()).hexdigest()[:8]
    return f"_clair_routing_{digest}"


def _load_routing_table(path: Path) -> dict[str, RoutingRule | None]:
    """Run a routing file and return its validated routing table.

    Args:
        path: Path to the ``__routing__.py`` file.

    Returns:
        The routing table, as a dict of environment name to routing rule.

    Raises:
        InvalidRoutingFileError: If clair cannot run the file, or the table is
            not in the expected shape.
    """
    cache_key = (str(path), path.stat().st_mtime_ns)
    cached = _routing_table_cache.get(cache_key)
    if cached is not None:
        return cached

    module_name = _module_name_for(path)
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise InvalidRoutingFileError(str(path), "clair cannot read this file")

    module = importlib.util.module_from_spec(spec)
    # Register the module before execution. A class or a dataclass in the file
    # then keeps one identity across loads.
    sys.modules[module_name] = module
    try:
        spec.loader.exec_module(module)
    except Exception as exc:
        del sys.modules[module_name]
        raise InvalidRoutingFileError(
            str(path), f"{type(exc).__name__}: {exc}"
        ) from exc

    if not hasattr(module, ROUTING_TABLE_ATTRIBUTE):
        raise InvalidRoutingFileError(
            str(path), f"the file must define a '{ROUTING_TABLE_ATTRIBUTE}' dict"
        )

    table = getattr(module, ROUTING_TABLE_ATTRIBUTE)
    if not isinstance(table, dict):
        raise InvalidRoutingFileError(
            str(path),
            f"'{ROUTING_TABLE_ATTRIBUTE}' must be a dict, "
            f"but it is a {type(table).__name__}",
        )

    for env_name, rule in table.items():
        if not isinstance(env_name, str):
            raise InvalidRoutingFileError(
                str(path),
                f"every key must be an environment name string, "
                f"but one key is a {type(env_name).__name__}",
            )
        if rule is None or isinstance(rule, RoutingConfig) or callable(rule):
            continue
        raise InvalidRoutingFileError(
            str(path),
            f"the rule for '{env_name}' is a {type(rule).__name__}. A rule must "
            "be a RoutingConfig, a callable, or None",
        )

    _routing_table_cache[cache_key] = table
    return table


def load_project_routing(project_root: Path, env_name: str) -> ProjectRouting:
    """Find the routing rule for one environment.

    A project without a ``__routing__.py`` gets passthrough routing. An
    environment without an entry in the table also gets passthrough routing, and
    the caller warns about it, because passthrough writes to production names.

    Args:
        project_root: Root directory of the clair project.
        env_name: Resolved environment name, such as "dev".

    Returns:
        A ``ProjectRouting`` with the rule, the file path, and all environment
        names that the table defines.

    Raises:
        InvalidRoutingFileError: If the file exists but clair cannot use it.
    """
    path = project_root / ROUTING_FILE_NAME
    if not path.exists():
        return ProjectRouting(
            rule=None, file_path=None, environment_names=[], has_entry=False
        )

    table = _load_routing_table(path)
    return ProjectRouting(
        rule=table.get(env_name),
        file_path=path,
        environment_names=sorted(table),
        has_entry=env_name in table,
    )
