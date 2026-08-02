"""Load the project routing file, ``__routing__.py``.

Routing lives in the project, not in ``~/.clair/environments.yml``:

* Routing is not a secret. A team commits it and reviews it like other code.
* Routing gains the most from Python. An entry reads an environment variable, so
  one committed entry gives each developer a separate target.
* A project-local file matches the clair version that the project pins.

The file defines a ``routing`` name and gives it a ``RoutingTable``. Each entry
in the table names one environment. That name is the join key: it matches a
top-level key in ``~/.clair/environments.yml``.
"""

from __future__ import annotations

import hashlib
import importlib.util
import sys
from pathlib import Path
from typing import NamedTuple

from clair.environments.routing import RoutingEntry, RoutingTable
from clair.exceptions import InvalidRoutingFileError

ROUTING_FILE_NAME = "__routing__.py"
ROUTING_TABLE_ATTRIBUTE = "routing"

# Cache key is (resolved path, modification time), so an edit reloads the file
# but repeated loads in one process do not run the file again. An entry that
# reads a keychain or a secret store must not run two times.
_routing_table_cache: dict[tuple[str, int], RoutingTable] = {}


class ProjectRouting(NamedTuple):
    """The outcome of a routing file lookup for one environment."""

    entry: RoutingEntry | None
    file_path: Path | None
    environment_names: list[str]
    has_entry: bool = False

    @property
    def file_exists(self) -> bool:
        """Tell the caller if the project has a ``__routing__.py``."""
        return self.file_path is not None

    @property
    def is_unnamed_environment(self) -> bool:
        """Tell the caller if the table omits this environment.

        An absent entry is almost always a typo. Clair then writes to the
        logical names, which are the production names.
        """
        return self.file_exists and not self.has_entry


def _module_name_for(path: Path) -> str:
    """Build a module name that is unique per routing file path."""
    digest = hashlib.md5(str(path).encode()).hexdigest()[:8]
    return f"_clair_routing_{digest}"


def _load_routing_table(path: Path) -> RoutingTable:
    """Run a routing file and give back its routing table.

    Args:
        path: The path of the ``__routing__.py`` file.

    Returns:
        The ``RoutingTable`` that the file defines.

    Raises:
        InvalidRoutingFileError: If clair cannot run the file, or the file does
            not define a ``RoutingTable``.
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
    if not isinstance(table, RoutingTable):
        raise InvalidRoutingFileError(
            str(path),
            f"'{ROUTING_TABLE_ATTRIBUTE}' must be a RoutingTable, "
            f"but it is a {type(table).__name__}",
        )

    _routing_table_cache[cache_key] = table
    return table


def load_project_routing(project_root: Path, env_name: str) -> ProjectRouting:
    """Find the routing entry for one environment.

    A project without a ``__routing__.py`` gets passthrough routing. An
    environment without an entry in the table also gets passthrough routing, and
    the caller warns about it, because passthrough writes to production names.

    Args:
        project_root: The root directory of the clair project.
        env_name: The resolved environment name, such as "dev".

    Returns:
        A ``ProjectRouting`` with the entry, the file path, and all the
        environment names that the table holds.

    Raises:
        InvalidRoutingFileError: If the file exists but clair cannot use it.
    """
    path = project_root / ROUTING_FILE_NAME
    if not path.exists():
        return ProjectRouting(
            entry=None, file_path=None, environment_names=[], has_entry=False
        )

    table = _load_routing_table(path)
    entry = table.entry_for(env_name)
    return ProjectRouting(
        entry=entry,
        file_path=path,
        environment_names=table.environment_names,
        has_entry=entry is not None,
    )
