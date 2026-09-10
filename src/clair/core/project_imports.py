"""Clair loads the files of one project as Python modules. This module owns that.

Every change that clair makes to the import system is here: the ``sys.path``
entries, the ``sys.modules`` names, and the ``sys.meta_path`` finder. One home,
because the three interact. ``make_project_importable()`` is the one entry
point, and ``discover_project()`` is the one caller.

Two properties make a project loadable, and each one needs the import system.

**A name means one thing.** A Trouve imports a different Trouve from the
project root, as ``from source.orders.raw import trouve``. That name is
meaningful inside one project, and ``sys.modules`` is global to the process. A
second project with the same layout therefore asks for a name that the first
project holds, and Python answers with the file of the first project. So clair
forgets the modules of the project before it. A notebook, the tests, and any
program that reads two projects need this.

**A file means one module.** Python keys ``sys.modules`` by the dotted name,
and not by the file, so one file imported under two names runs two times and
gives two module objects. For clair that fault is fatal, and it is silent: each
execution makes its own ``Trouve`` objects, discovery knows one of them, the SQL
of the author points to the other, and the DAG loses the edge. See
``describe_unresolved_tokens()`` in ``core/references.py`` for the symptom.

The finder removes the second fault at its origin. It resolves an import with
the normal ``sys.path`` machinery, and then it reads the file path of the
result. If a module object for that file exists, the import receives that object
under the new name, and the file does not run again. Thus the import of the
author and the import of discovery agree on one object, whatever name each of
them uses. The standard library does the same by hand: ``os.path`` and
``posixpath`` are two names for one module.

The finder touches an import only when the file sits below a project root that
clair loads. Every other import goes through the normal machinery, with no
change.
"""

from __future__ import annotations

import importlib.abc
import importlib.machinery
import importlib.util
import re
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

# The root of each project that this process loaded. The next project forgets
# the modules of each of them.
_loaded_project_roots: set[Path] = set()

# The sys.path entries that clair inserted. This set is separate from
# _loaded_project_roots: a package anchor can hold the library modules of a
# whole monorepo, and clair must never take those out of sys.modules.
_inserted_sys_path_entries: set[str] = set()


def make_project_importable(project_root: Path) -> None:
    """Prepare the import system to load the files of *project_root*.

    The function does three steps, in this order:

    1. Forget each project that this process loaded before, because two
       projects can ask for one module name.
    2. Put the project root on ``sys.path``, and the package anchor too when
       the project sits inside a Python package.
    3. Install the finder, thus one file gives one module object.

    The function makes no step to undo this at the end of discovery. A pandas
    transform runs long after discovery, and it can import a helper module of
    its project at that moment. The import system therefore keeps the project
    until the next project replaces it.
    """
    project_root = project_root.resolve()
    _forget_loaded_projects(project_root)

    package_anchor = _package_anchor(project_root)
    _insert_sys_path_entry(project_root)
    if package_anchor is not None:
        _insert_sys_path_entry(package_anchor)
        _forget_package_chain(project_root, package_anchor)

    _loaded_project_roots.add(project_root)
    _watched_project_roots.add(project_root)
    _install_finder()


def _insert_sys_path_entry(directory: Path) -> None:
    entry = str(directory)
    if entry not in sys.path:
        sys.path.insert(0, entry)
        _inserted_sys_path_entries.add(entry)


def _package_anchor(project_root: Path) -> Path | None:
    """Give the directory above the package that holds *project_root*, or None.

    A project root that holds ``__init__.py`` is part of a Python package. The
    author of such a project imports a Trouve file through that package, for
    example ``from clair_projects.analytics.source.orders.raw import trouve``.
    That import needs the directory above the package on ``sys.path``. The
    function walks up while ``__init__.py`` exists, in the same way that pytest
    finds the package root of a test file.
    """
    if not (project_root / "__init__.py").is_file():
        return None
    anchor = project_root
    while (anchor.parent / "__init__.py").is_file():
        anchor = anchor.parent
    return anchor.parent


def _forget_package_chain(project_root: Path, package_anchor: Path) -> None:
    """Remove the packages between *package_anchor* and *project_root*.

    A parent package of the project can sit in ``sys.modules`` from an earlier
    project, with a ``__path__`` that points to another tree. Python does not
    recalculate the ``__path__`` of a regular package, thus an import through
    that package would read the files of the earlier project. Clair removes the
    chain, and the next import reads ``sys.path`` again.
    """
    chain_parts = project_root.relative_to(package_anchor).parts
    for depth in range(1, len(chain_parts) + 1):
        sys.modules.pop(".".join(chain_parts[:depth]), None)


def _forget_loaded_projects(next_project_root: Path) -> None:
    """Remove each module and each ``sys.path`` entry of the projects before.

    The function also forgets the modules of *next_project_root* itself, thus a
    second discovery of one project reads the files again. A notebook that
    edits a Trouve file and runs the project again needs that.
    """
    roots = {next_project_root, *_loaded_project_roots}

    # Read the locations of every module first, and delete after. A namespace
    # package reads its parent module when it gives its path list, thus a
    # deletion in the middle of the loop hides the modules that come after it.
    locations_of = {
        module_name: _module_locations(module)
        for module_name, module in list(sys.modules.items())
    }
    for module_name, locations in locations_of.items():
        if any(_is_inside(location, root) for location in locations for root in roots):
            sys.modules.pop(module_name, None)

    for entry in list(_inserted_sys_path_entries - {str(next_project_root)}):
        if entry in sys.path:
            sys.path.remove(entry)
        _inserted_sys_path_entries.discard(entry)

    _loaded_project_roots.clear()
    _watched_project_roots.clear()
    _resolved_path_cache.clear()


def _module_locations(module: object) -> list[str]:
    """Give each file path and each directory path of one module.

    A module gives ``__file__``. A package, a namespace package too, gives
    ``__path__``. A namespace package has no ``__file__``, thus the path list
    is the one way to find the project that it belongs to.
    """
    locations: list[str] = []
    module_file = getattr(module, "__file__", None)
    if module_file:
        locations.append(str(module_file))
    # A namespace package recalculates its path list, and that step reads the
    # parent module. An earlier project can remove that parent, and the read
    # then fails. Such a module has no path that a caller can use, thus an
    # empty list is the correct answer.
    try:
        module_path = list(getattr(module, "__path__", []))
    except Exception:  # noqa: BLE001 -- the import machinery raises many types here
        module_path = []
    locations.extend(str(entry) for entry in module_path)
    return locations


def _is_inside(location: str, root: Path) -> bool:
    """Tell you if *location* is the root directory, or a path below it."""
    try:
        Path(location).relative_to(root)
    except ValueError:
        return False
    return True


def load_project_file(file_path: Path, module_name: str) -> ModuleType | None:
    """Give the module of *file_path*, and run the file if nothing runs it yet.

    An earlier candidate can import this file as a dependency, under
    *module_name* or under another name. The file identifies the module, thus
    the function gives that module back and the file does not run again.

    Returns None when Python gives no loader for the file. Raises the error of
    the file itself, because the caller reports each fault together.
    """
    existing_module = module_for_file(file_path)
    if existing_module is not None:
        return existing_module

    spec = importlib.util.spec_from_file_location(module_name, file_path)
    if spec is None or spec.loader is None:
        return None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    try:
        spec.loader.exec_module(module)
    except BaseException:
        # The file stopped in the middle, thus the module holds a part of the
        # names that it declares. Remove it, in the same way as the import
        # machinery of Python. A later candidate must never adopt it.
        sys.modules.pop(module_name, None)
        raise
    return module


def load_support_file(file_path: Path) -> ModuleType | None:
    """Run a clair configuration file, and give its module.

    A configuration file declares no Trouve — ``__database_config__.py`` and
    ``__schema_config__.py`` are the two. The name of the module comes from the
    complete path, thus two projects of one monorepo never take one name. A
    name from the path below the project root would collide, and the second
    project would then read the configuration of the first.

    Returns None when Python gives no loader for the file. Raises the error of
    the file itself.
    """
    sanitized = re.sub(r"\W", "_", str(file_path.with_suffix("")))
    module_name = f"_clair_config_{sanitized}"

    spec = importlib.util.spec_from_file_location(module_name, file_path)
    if spec is None or spec.loader is None:
        return None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    try:
        spec.loader.exec_module(module)
    except BaseException:
        sys.modules.pop(module_name, None)
        raise
    return module


def names_of_a_file_that_ran_two_times(file_path: Path) -> list[str]:
    """Give the module names of *file_path* when the file gave two module objects.

    Two names for one file are correct: the finder gives the second name the
    module of the first, in the same way that ``os.path`` and ``posixpath``
    name one module. Two **objects** are the fault, because each object holds
    its own Trouve. The function therefore counts the objects, and it answers
    an empty list for a file that ran one time.
    """
    names_of_object: dict[int, list[str]] = {}
    target = file_path.resolve()
    for module_name, module in list(sys.modules.items()):
        module_file = getattr(module, "__file__", None)
        if not module_file:
            continue
        try:
            if Path(module_file).resolve() != target:
                continue
        except OSError:
            continue
        names_of_object.setdefault(id(module), []).append(module_name)
    if len(names_of_object) < 2:
        return []
    return sorted(name for names in names_of_object.values() for name in names)


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

    def is_package(self, fullname: str) -> bool:
        """Tell the import machinery that a package stays a package.

        ``spec_from_loader`` reads this method. Without it the new spec holds
        no search location, thus ``import <alias>.submodule`` would fail.
        """
        return hasattr(self._module, "__path__")

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
        alias_spec = importlib.util.spec_from_loader(
            fullname, _AliasLoader(existing_module), origin=spec.origin
        )
        if alias_spec is not None and alias_spec.submodule_search_locations is not None:
            # A package keeps the search path of the module that runs the file,
            # thus a submodule of the alias resolves in the same directory.
            alias_spec.submodule_search_locations = list(
                getattr(existing_module, "__path__", [])
            )
        return alias_spec


_finder = _OneModulePerFileFinder()


def _install_finder() -> None:
    if _finder not in sys.meta_path:
        sys.meta_path.insert(0, _finder)
