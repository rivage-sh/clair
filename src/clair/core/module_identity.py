"""One file below a project root gives one module object, under every import name.

Python keys ``sys.modules`` by the dotted module name, and not by the file. One
file imported under two names therefore runs two times and gives two module
objects. For clair that fault is fatal, and it is silent: each execution makes
its own ``Trouve`` objects, discovery knows one of them, the SQL of the author
points to the other, and the DAG loses the edge between the two files. See
``_describe_unresolved_tokens`` in ``core/discovery.py`` for the symptom.

The finder here removes the fault at its origin. It resolves an import with the
normal ``sys.path`` machinery, and then it reads the file path of the result. If
a module object for that file exists, the import receives that object under the
new name, and the file does not run again. Thus the import of the author and
the import of discovery agree on one object, whatever name each of them uses.
The standard library does the same by hand: ``os.path`` and ``posixpath`` are
two names for one module.

The finder touches an import only when the file sits below a project root that
``watch_project_root()`` names. Every other import goes through the normal
machinery, with no change.
"""

from __future__ import annotations

import importlib.abc
import importlib.machinery
import importlib.util
import sys
from collections.abc import Sequence
from pathlib import Path
from types import ModuleType

# The resolved path of each project root that discovery loaded. The finder
# reads it on each import, and it stands aside when the set is empty.
_watched_project_roots: set[Path] = set()

# Path.resolve() makes system calls. One import scans every module in
# sys.modules, thus this cache keys the resolved path by the raw __file__
# string.
_resolved_path_cache: dict[str, Path] = {}


def watch_project_root(project_root: Path) -> None:
    """Give the finder one more project root, and install it at the first call."""
    _watched_project_roots.add(project_root.resolve())
    _install_finder()


def unwatch_every_project_root() -> None:
    """Empty the set of project roots. The finder then stands aside."""
    _watched_project_roots.clear()
    _resolved_path_cache.clear()


def _resolve(raw_path: str) -> Path:
    resolved = _resolved_path_cache.get(raw_path)
    if resolved is None:
        resolved = Path(raw_path).resolve()
        _resolved_path_cache[raw_path] = resolved
    return resolved


def module_for_file(file_path: Path) -> ModuleType | None:
    """Give the module object that runs *file_path*, or None.

    ``sys.modules`` is the one authority. The function scans it, thus a module
    that any importer loaded — discovery, or an import in a Trouve file — is
    the answer, and no second registry can go stale.
    """
    target = _resolve(str(file_path))
    for module in list(sys.modules.values()):
        module_file = getattr(module, "__file__", None)
        if module_file and _resolve(module_file) == target:
            return module
    return None


def _is_below_a_watched_root(file_path: Path) -> bool:
    for project_root in _watched_project_roots:
        if file_path.is_relative_to(project_root):
            return True
    return False


class _AliasLoader(importlib.abc.Loader):
    """Give an existing module to an import that uses a different name.

    The import machinery sets ``__name__`` and ``__spec__`` on the module that
    ``create_module`` gives back. The loader restores both in ``exec_module``,
    thus the module keeps the name of its first import.
    """

    def __init__(self, module: ModuleType) -> None:
        self._module = module
        self._original_name = module.__name__
        self._original_spec = module.__spec__

    def create_module(self, spec: importlib.machinery.ModuleSpec) -> ModuleType:
        return self._module

    def exec_module(self, module: ModuleType) -> None:
        # The file ran under its first name. Run nothing.
        module.__name__ = self._original_name
        module.__spec__ = self._original_spec


class _OneModulePerFileFinder(importlib.abc.MetaPathFinder):
    """The ``sys.meta_path`` finder. The module docstring gives the design."""

    def find_spec(
        self,
        fullname: str,
        path: Sequence[str] | None = None,
        target: ModuleType | None = None,
    ) -> importlib.machinery.ModuleSpec | None:
        if not _watched_project_roots:
            return None
        spec = importlib.machinery.PathFinder.find_spec(fullname, path, target)
        if spec is None or spec.origin is None or not spec.has_location:
            return None
        file_path = _resolve(spec.origin)
        if not _is_below_a_watched_root(file_path):
            return None
        existing_module = module_for_file(file_path)
        if existing_module is None:
            # The first import of this file. The normal machinery loads it, and
            # sys.modules then holds it for the next name.
            return None
        return importlib.util.spec_from_loader(
            fullname, _AliasLoader(existing_module), origin=spec.origin
        )


_finder = _OneModulePerFileFinder()


def _install_finder() -> None:
    if _finder not in sys.meta_path:
        sys.meta_path.insert(0, _finder)
