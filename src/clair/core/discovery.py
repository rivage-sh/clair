"""Project discovery. Clair reads the project root and loads each Trouve file."""

from __future__ import annotations

import importlib.util
import os
import re
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import TYPE_CHECKING

import structlog

import clair as _clair_pkg

if TYPE_CHECKING:
    from clair.environments.environments import Environment

from clair.core import module_identity
from clair.environments.project_routing import ROUTING_FILE_NAME
from clair.environments.routing import (
    RoutingEntry,
    TrouveAddress,
    detect_routing_collisions,
    route,
)
from clair.exceptions import (
    DiscoveryError,
    NotAProjectRootError,
    ProjectDiscoveryError,
    ProjectRootNotFoundError,
)
from clair.trouves._refs import THIS_PLACEHOLDER, TROUVE_PLACEHOLDER_PREFIX
from clair.trouves._refs import clear as clear_refs
from clair.trouves.config import DatabaseDefaults, ResolvedConfig, SchemaDefaults
from clair.trouves.dataframe_trouve import DataframeTrouve
from clair.trouves.run_config import RunMode
from clair.trouves.test import TestSql
from clair.trouves.trouve import CompiledAttributes, ExecutionType, Trouve, TrouveAbc, TrouveType

ARTIFACTS_DIR_NAME = "_clairtifacts"
_SKIP_DIRS = {"clair", "tests", ARTIFACTS_DIR_NAME, "__pycache__", ".git", ".venv", "node_modules"}
_CONFIG_FILES = {"__database_config__.py", "__schema_config__.py"}

logger = structlog.get_logger()


TROUVE_DEPTH = 3
"""The number of path parts that a Trouve file holds below its project root.

The parts are database_name/schema_name/table_name.py. A file above that depth
holds no schema name and no database name, thus clair cannot make its address.
"""


def find_project_root(start_directory: Path) -> Path:
    """Give the first directory at or above *start_directory* with ``__routing__.py``.

    That file sits at the root of every clair project, thus it marks the root,
    in the same way that ``.git`` marks a git repository. A user therefore runs
    a clair command from any directory of the project.

    Raises:
        ProjectRootNotFoundError: No directory at or above *start_directory*
            holds the marker file.
    """
    start_directory = start_directory.resolve()
    for directory in [start_directory, *start_directory.parents]:
        if (directory / ROUTING_FILE_NAME).is_file():
            return directory
    raise ProjectRootNotFoundError(str(start_directory), ROUTING_FILE_NAME)


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


def compute_logical_address(file_path: Path) -> TrouveAddress:
    """Make the logical address from the last three parts of the path.

    Example: .../database_name/schema_name/table_name.py becomes
    database_name.schema_name.table_name

    A directory above the database directory takes no part in the address. Thus
    a project inside a larger repository keeps the addresses that it declares.
    Call ``describe_shallow_trouve`` first: this function reads the parts of the
    path, and a file too near the root would take a part from outside the
    project.
    """
    return TrouveAddress.parse(".".join(file_path.with_suffix("").parts[-3:]))


def describe_shallow_trouve(file_path: Path, project_root: Path) -> str | None:
    """Tell you why a file is too near the project root, or give None.

    The address of a Trouve comes from the last three parts of its path. A file
    with fewer than three parts below the root would take a part from a
    directory outside the project. The address would then hold the name of the
    parent directory of the project, and the same project would write to two
    different tables on two machines.
    """
    parts = file_path.relative_to(project_root).parts
    if len(parts) >= TROUVE_DEPTH:
        return None
    location = "the project root" if len(parts) == 1 else "/".join(parts[:-1])
    return (
        f"{file_path}: a Trouve file sits {TROUVE_DEPTH} levels below the project "
        f"root, as database_name/schema_name/table_name.py. This file sits in "
        f"{location}, which gives it no database name and no schema name. Move "
        f"the file, or give the file a name that starts with _ to hide it from "
        f"discovery."
    )


def _is_trouve_candidate(file_path: Path) -> bool:
    if file_path.name.startswith("_"):
        return False
    return file_path.suffix == ".py"


def _config_module_name(file_path: Path) -> str:
    """Make a sys.modules name for a config file.

    The name comes from the complete path, thus two projects of one monorepo
    never take one name. A name from the path below the project root would
    collide, and the second project would then read the config of the first.
    """
    sanitized = re.sub(r"\W", "_", str(file_path.with_suffix("")))
    return f"_clair_config_{sanitized}"


def _load_config_file(file_path: Path) -> DatabaseDefaults | SchemaDefaults | None:
    if not file_path.exists():
        return None
    module_name = _config_module_name(file_path)
    try:
        spec = importlib.util.spec_from_file_location(module_name, file_path)
        if spec is None or spec.loader is None:
            return None
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        spec.loader.exec_module(module)
        defaults = getattr(module, "defaults", None)
        if isinstance(defaults, (DatabaseDefaults, SchemaDefaults)):
            return defaults
    except Exception as e:  # noqa: BLE001 — the user config code is unknown, but it is never fatal
        logger.debug("discovery.config_load_error", file=str(file_path), error=str(e))
    return None


def _resolve_config(
    file_path: Path,
    profile_defaults: dict[str, str | None] | None = None,
) -> ResolvedConfig:
    """Make the merged config of a Trouve. The function moves up the directory tree.

    The function reads these sources in order. Each source replaces the values
    of the source before it:
    1. The profile defaults
    2. __database_config__.py
    3. __schema_config__.py

    The function starts at the file and moves up, in the same direction as
    ``compute_logical_address``. The schema directory is the parent of the file,
    and the database directory is the parent of the schema directory. Thus a
    project below other directories keeps its config, and the config directory
    is always the directory that the address names.
    """
    profile_wh = (profile_defaults or {}).get("warehouse")
    profile_role = (profile_defaults or {}).get("role")
    config = ResolvedConfig(
        warehouse=profile_wh if profile_wh and profile_wh.strip() else None,
        role=profile_role if profile_role and profile_role.strip() else None,
    )

    schema_directory = file_path.parent
    database_directory = schema_directory.parent

    db_defaults = _load_config_file(database_directory / "__database_config__.py")
    if isinstance(db_defaults, DatabaseDefaults):
        if db_defaults.warehouse and db_defaults.warehouse.strip():
            config.warehouse = db_defaults.warehouse
        if db_defaults.role and db_defaults.role.strip():
            config.role = db_defaults.role

    schema_defaults = _load_config_file(schema_directory / "__schema_config__.py")
    if isinstance(schema_defaults, SchemaDefaults):
        if schema_defaults.warehouse and schema_defaults.warehouse.strip():
            config.warehouse = schema_defaults.warehouse
        if schema_defaults.role and schema_defaults.role.strip():
            config.role = schema_defaults.role

    return config


_PLACEHOLDER_RE = re.compile(re.escape(TROUVE_PLACEHOLDER_PREFIX) + r"(\d+)")


def _resolve_sql(
    sql: str,
    id_to_address: dict[int, TrouveAddress],
    this_address: TrouveAddress,
) -> str:
    """Render the SQL of the author into SQL with true addresses.

    The function replaces a token that points to a different Trouve
    (``__CLAIR_TROUVE_<id>__``) with the address in ``id_to_address``. It also
    replaces the THIS marker (``__CLAIR_THIS__``) with ``this_address``.

    Clair calls this function two times, and the map decides the difference.
    discover_project() gives the logical addresses, because it does not know the
    selection. recompile_for_selection() gives the address that the selection
    decides. Both calls read the same source string, thus the second call needs
    no text substitution on the result of the first.
    """
    def replace(m: re.Match[str]) -> str:
        address = id_to_address.get(int(m.group(1)))
        return str(address) if address else m.group(0)
    result = _PLACEHOLDER_RE.sub(replace, sql)
    return result.replace(THIS_PLACEHOLDER, str(this_address))


def _detect_imports(
    sql: str,
    id_to_logical_address: dict[int, TrouveAddress],
    own_logical_address: TrouveAddress,
) -> list[str]:
    """Give the logical address of each Trouve that the SQL points to with a token."""
    imports: list[str] = []
    for obj_id_str in _PLACEHOLDER_RE.findall(sql):
        dependency = id_to_logical_address.get(int(obj_id_str))
        if dependency is None or dependency == own_logical_address:
            continue
        if str(dependency) not in imports:
            imports.append(str(dependency))
    return imports



# The root of each project that this process discovered. A second discovery
# removes the modules of the projects before it.
_loaded_project_roots: set[str] = set()

# The sys.path entries that clair inserted. This set is separate from
# _loaded_project_roots: a package anchor can hold the library modules of a
# whole monorepo, and a discovery must never take those out of sys.modules.
_inserted_sys_path_entries: set[str] = set()


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
    # parent module. An earlier discovery can remove that parent, and the read
    # then fails. Such a module has no path that a caller can use, thus an
    # empty list is the correct answer.
    try:
        module_path = list(getattr(module, "__path__", []))
    except Exception:  # noqa: BLE001 -- the import machinery raises many types here
        module_path = []
    locations.extend(str(entry) for entry in module_path)
    return locations


def _forget_project_modules(project_root: Path) -> None:
    """Remove each Trouve module of a clair project from ``sys.modules``.

    Two projects can give one module the same name. The probe projects of the
    tests do this, and a notebook that runs two projects does it too. Python
    keeps the first module under that name, so the second discovery would read
    the files of the first project. The function therefore removes the modules
    of each project that this process loaded, and takes the other project roots
    off ``sys.path``.
    """
    roots = {str(project_root), *_loaded_project_roots}

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

    for entry in list(_inserted_sys_path_entries - {str(project_root)}):
        if entry in sys.path:
            sys.path.remove(entry)
        _inserted_sys_path_entries.discard(entry)
    _loaded_project_roots.clear()
    module_identity.unwatch_every_project_root()


def _is_inside(location: str, root: str) -> bool:
    """Tell you if *location* is the root directory, or a path below it."""
    try:
        Path(location).relative_to(root)
    except ValueError:
        return False
    return True


def discover_project(
    project_root: Path,
    profile_defaults: dict[str, str | None] | None = None,
    routing: RoutingEntry | None = None,
    environment: Environment | None = None,
    run_mode: RunMode | None = None,
) -> list[TrouveAbc]:
    """Find each Trouve in a project.

    The function reads the project root and loads each Trouve file. It replaces
    the SQL placeholders, finds the import relations, and gives the compiled
    Trouve objects.

    Args:
        project_root: The absolute path of the project root directory.
        profile_defaults: The default warehouse and role from the active profile.
        routing: The routing entry that makes each physical address. It comes
            from __routing__.py.
        environment: The active environment. Clair puts it in ``clair.env``.
            Thus a Trouve module can read it at load time, for a feature flag.
        run_mode: The run mode that the user asks for: FULL_REFRESH or
            INCREMENTAL. Clair puts it in ``clair.run_mode``. Thus a Trouve
            module can read it at load time and change its WHERE clause.

    Returns:
        A list of Trouve objects. Each object has a value in .compiled.
    """
    project_root = project_root.resolve()

    # Put the active environment and the run mode on the clair package. Thus a
    # Trouve module can read them at load time, for example with
    # ``import clair; clair.env.role``.
    _clair_pkg.env = environment
    _clair_pkg.run_mode = run_mode

    # The marker check. Without it, clair reads a directory that holds many
    # projects as one project, and it builds one DAG from all of them.
    if not (project_root / ROUTING_FILE_NAME).is_file():
        raise NotAProjectRootError(str(project_root), ROUTING_FILE_NAME)

    # Empty the refs registry and remove the modules of each project that this
    # process loaded. Thus each discovery run starts from a clean state.
    clear_refs()
    _forget_project_modules(project_root)

    # Put the project root in sys.path. Thus an import of a different Trouve
    # from the project root works. A project inside a Python package also gets
    # its package anchor, thus an import through the package works too.
    project_root_str = str(project_root)
    sys_path_entries = [project_root_str]
    package_anchor = _package_anchor(project_root)
    if package_anchor is not None:
        sys_path_entries.append(str(package_anchor))
        # A parent package of the project can sit in sys.modules from an
        # earlier discovery, with a __path__ that points to another tree.
        # Python does not recompute the __path__ of a regular package, thus
        # clair removes the chain, and the next import reads sys.path again.
        chain_parts = project_root.relative_to(package_anchor).parts
        for depth in range(1, len(chain_parts) + 1):
            sys.modules.pop(".".join(chain_parts[:depth]), None)
    for entry in sys_path_entries:
        if entry not in sys.path:
            sys.path.insert(0, entry)
            _inserted_sys_path_entries.add(entry)
    _loaded_project_roots.add(project_root_str)

    # One file below the project root gives one module object, whatever name an
    # import uses. Without this, one file imported under two names runs two
    # times, and the second Trouve object breaks the DAG. See module_identity.
    module_identity.watch_project_root(project_root)

    # Collect the candidate files.
    candidates: list[Path] = []
    for dirpath, dirnames, filenames in os.walk(project_root):
        dirnames[:] = [d for d in dirnames if d not in _SKIP_DIRS and not d.startswith("_")]
        for filename in filenames:
            file_path = Path(dirpath) / filename
            if _is_trouve_candidate(file_path):
                candidates.append(file_path)
    candidates.sort()

    # Load each candidate. A file can be in sys.modules already, because an
    # earlier candidate imported it as a dependency.
    collected: list[tuple[TrouveAbc, TrouveAddress, Path, str]] = []
    errors: list[str] = []

    for file_path in candidates:
        module_name = str(
            file_path.relative_to(project_root).with_suffix("")
        ).replace(os.sep, ".")

        # An earlier candidate can import this file as a dependency, under this
        # name or under another name. The file identifies the module, thus one
        # file gives one module object and one Trouve object.
        already_loaded_module = module_identity.module_for_file(file_path)
        if already_loaded_module is not None:
            module = already_loaded_module
            module_name = module.__name__
        else:
            try:
                spec = importlib.util.spec_from_file_location(module_name, file_path)
                if spec is None or spec.loader is None:
                    continue
                module = importlib.util.module_from_spec(spec)
                sys.modules[module_name] = module
                spec.loader.exec_module(module)
            except Exception as e:  # noqa: BLE001 — the user module code is unknown; clair reports the fault as an error
                logger.warning("discovery.load_error", file=str(file_path), error=str(e))
                errors.append(f"{file_path}: {e}")
                continue

        trouve_obj = getattr(module, "trouve", None)
        if not isinstance(trouve_obj, TrouveAbc):
            continue

        # The depth rule applies to a Trouve file, and not to each Python file.
        # A project can hold a script or a helper near its root, and that file
        # declares no Trouve.
        shallow = describe_shallow_trouve(file_path, project_root)
        if shallow:
            errors.append(shallow)
            continue

        collected.append(
            (trouve_obj, compute_logical_address(file_path), file_path, module_name)
        )

    # Phase A: make the logical address and the physical address of each Trouve.
    # The logical address comes from the file path. DAG edges and selectors use it.
    # The physical address is the target. The SQL and the DDL use it.
    # The routing entry sees every Trouve, a SOURCE too.
    logical_addresses: dict[int, TrouveAddress] = {}
    physical_addresses: dict[int, TrouveAddress] = {}
    collision_check: dict[str, TrouveAddress] = {}

    for trouve_obj, logical_address, _, _ in collected:
        logical_addresses[id(trouve_obj)] = logical_address
        physical_address = route(logical_address, trouve_obj.type, routing)
        physical_addresses[id(trouve_obj)] = physical_address
        # A TABLE that routes onto a SOURCE replaces the data that it reads.
        collision_check[str(logical_address).upper()] = physical_address

    # Make a map from an id to a logical address, for the pandas dependencies.
    # With this map, clair finds the logical address of each Trouve that a
    # DataframeTrouve names in its inputs.
    id_to_logical_address: dict[int, TrouveAddress] = {
        id(trouve_obj): logical_addresses[id(trouve_obj)]
        for trouve_obj, _, _, _ in collected
    }

    # Phase B: compile each Trouve.
    # Clair puts the logical addresses in the SQL. Thus, by default, the SQL
    # reads the production upstream tables. After the selection, call
    # recompile_for_selection() to change each selected upstream address to its
    # physical address.
    for trouve_obj, _, file_path, module_name in collected:
        logical = logical_addresses[id(trouve_obj)]
        physical = physical_addresses[id(trouve_obj)]

        if trouve_obj.execution_type == ExecutionType.PANDAS:
            assert isinstance(trouve_obj, DataframeTrouve)
            transform_imports: list[str] = []
            # The address that each input reads, in the parameter order of the
            # transform. This list is the counterpart of the addresses in the
            # SQL of a SQL Trouve: discovery writes the logical address, and
            # recompile_for_selection() changes it in the same way. Thus the two
            # backends read the same tables.
            input_addresses: list[str] = []
            for upstream in trouve_obj.upstream_trouves():
                dependency = id_to_logical_address.get(id(upstream))
                if dependency is None:
                    raise DiscoveryError(
                        str(file_path),
                        f"the Trouve '{logical}' names an input that clair did "
                        "not find. Each input must be the `trouve` object of a "
                        "file in this project.",
                    )
                input_addresses.append(str(dependency))

                if dependency == logical:
                    continue
                if str(dependency) not in transform_imports:
                    transform_imports.append(str(dependency))

            resolved_transform = trouve_obj.source_text()

            trouve_obj.compiled = CompiledAttributes(
                physical_address=physical,
                logical_address=logical,
                resolved_sql="",
                resolved_transform=resolved_transform,
                file_path=file_path.relative_to(project_root),
                module_name=module_name,
                imports=transform_imports,
                input_addresses=input_addresses,
                config=_resolve_config(file_path, profile_defaults),
                execution_type=ExecutionType.PANDAS,
            )
            for test in trouve_obj.tests:
                if isinstance(test, TestSql):
                    test.resolved_sql = _resolve_sql(
                        test.sql, logical_addresses, this_address=logical
                    )
        else:
            assert isinstance(trouve_obj, Trouve)
            trouve_obj.compiled = CompiledAttributes(
                physical_address=physical,
                logical_address=logical,
                resolved_sql=_resolve_sql(trouve_obj.sql, logical_addresses, this_address=logical),
                file_path=file_path.relative_to(project_root),
                module_name=module_name,
                imports=_detect_imports(trouve_obj.sql, logical_addresses, logical),
                config=_resolve_config(file_path, profile_defaults),
                execution_type=ExecutionType.SNOWFLAKE,
            )
            for test in trouve_obj.tests:
                if isinstance(test, TestSql):
                    test.resolved_sql = _resolve_sql(
                        test.sql, logical_addresses, this_address=logical
                    )

    errors.extend(_describe_unresolved_tokens(collected))

    trouve_count = len(collected)
    logger.info("discovery.complete", project_root=str(project_root), trouves=trouve_count, errors=len(errors))

    # A fault stops the run. A file that clair cannot read holds a Trouve that
    # the DAG then misses, and a run would report success after it built fewer
    # tables than the project declares.
    if errors:
        raise ProjectDiscoveryError(errors)

    return [trouve for trouve, _, _, _ in collected]


def _names_of_a_file_that_ran_two_times(file_path: Path) -> list[str]:
    """Give the module names of *file_path* when the file gave two module objects.

    Two names for one file are correct: the module identity finder gives the
    second name the module of the first, in the same way that ``os.path`` and
    ``posixpath`` name one module. Two **objects** are the fault, because each
    object holds its own Trouve. The function therefore counts the objects, and
    it answers an empty list for a file that ran one time.
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


def _describe_unresolved_tokens(
    collected: Sequence[tuple[TrouveAbc, TrouveAddress, Path, str]],
) -> list[str]:
    """Give one fault for each Trouve that keeps a placeholder token.

    A token stays in the SQL when clair holds no address for the object that
    the author interpolated. The module identity finder removes the common
    cause — one file that runs two times under two names — but an import
    machinery that clair does not see, for example the finder of an editable
    install, can still make a second module object. Clair must never send such
    SQL to the warehouse: the warehouse answers with a parse error that names
    the token and nothing else, and the DAG has already lost the edge in
    silence.
    """
    # A file that ran two times is the probable cause, and that file is the
    # referenced file, not the file that keeps the token. Name each such file.
    duplicates = {
        file_path.name: names
        for _, _, file_path, _ in collected
        if (names := _names_of_a_file_that_ran_two_times(file_path))
    }
    duplicate_text = ""
    if duplicates:
        listed = "; ".join(
            f"{file_name} as " + " and ".join(names)
            for file_name, names in sorted(duplicates.items())
        )
        duplicate_text = f" Python ran one file two times: {listed}."

    faults: list[str] = []
    for trouve_obj, logical_address, file_path, _ in collected:
        compiled = trouve_obj.compiled
        if compiled is None:
            continue
        texts = [compiled.resolved_sql]
        texts.extend(
            test.resolved_sql
            for test in trouve_obj.tests
            if isinstance(test, TestSql) and test.resolved_sql
        )
        tokens = sorted({
            match.group(0) for text in texts for match in _PLACEHOLDER_RE.finditer(text)
        })
        if not tokens:
            continue
        faults.append(
            f"{file_path}: the Trouve '{logical_address}' interpolates a "
            "Trouve object that clair did not collect, thus a reference token "
            "stays in the SQL. Clair stops, because the warehouse cannot read "
            "that SQL." + duplicate_text
        )
    return faults


def find_routing_collisions(trouves: Sequence[TrouveAbc]) -> list[tuple[str, list[str]]]:
    """Give a (physical_target, [logical_sources]) pair for each routing collision.

    A collision occurs when two Trouves that are not SOURCE Trouves route to one
    physical address. Call this function after discover_project(), to show each
    collision to the user.

    The result is an empty list when no routing policy is active. Then the
    logical address and the physical address are equal for each Trouve.
    """
    logical_to_physical = {
        str(trouve.compiled.logical_address): str(trouve.compiled.physical_address)
        for trouve in trouves
        if trouve.compiled and trouve.type != TrouveType.SOURCE
    }
    return detect_routing_collisions(logical_to_physical)


def _reference_addresses_for_selection(
    trouves: Sequence[TrouveAbc], selected_addresses: set[str]
) -> dict[int, TrouveAddress]:
    """Give the address that each Trouve reads at, keyed by the object id.

    Three rules decide the address:

    * This run builds the Trouve, thus a reader takes the physical address. The
      new data goes there.
    * This run does not build the Trouve, thus a reader takes the logical
      address. Nothing writes a new copy, thus the production table holds the
      newest data.
    * The Trouve is a SOURCE, thus a reader takes the physical address. Clair
      never builds a SOURCE, thus the routing entry is the only statement about
      where the data is.
    """
    reference_addresses: dict[int, TrouveAddress] = {}
    for trouve in trouves:
        if not trouve.compiled:
            continue
        this_run_builds_it = (
            str(trouve.compiled.physical_address) in selected_addresses
        )
        if trouve.type == TrouveType.SOURCE or this_run_builds_it:
            reference_addresses[id(trouve)] = trouve.compiled.physical_address
        else:
            reference_addresses[id(trouve)] = trouve.compiled.logical_address
    return reference_addresses


def recompile_for_selection(
    trouves: Sequence[TrouveAbc], selected_addresses: set[str]
) -> None:
    """Resolve each address again, now that clair knows the selection.

    discover_project() resolves each reference to a logical production address,
    because it does not know the selection yet. This function resolves each
    reference a second time, and the selection now decides each address. See
    ``_reference_addresses_for_selection`` for the rule.

    The function reads the placeholder tokens of the author, and not the
    addresses that discovery wrote. ``Trouve.sql`` and ``TestSql.sql`` keep
    those tokens, thus clair renders the SQL again from the source. Only a token
    becomes an address. An address that the author types as text stays as it is,
    and it makes no DAG edge either.

    This function changes each Trouve in place. It writes three places: the
    resolved_sql of a SQL Trouve, the resolved_sql of each TestSql, and the
    input_addresses of a DataFrame Trouve. It changes nothing for a Trouve outside
    the selection, because this run does not execute that Trouve.

    Args:
        trouves: Each Trouve from discover_project().
        selected_addresses: The physical addresses of the Trouves for this run.
            The DAG selector gives them.
    """
    reference_addresses = _reference_addresses_for_selection(
        trouves, selected_addresses
    )

    for trouve in trouves:
        if not trouve.compiled:
            continue
        if str(trouve.compiled.physical_address) not in selected_addresses:
            continue

        # The Trouve writes to its own physical address, thus its own SQL points
        # to the physical address too. An incremental Trouve reads the target
        # with the THIS marker.
        this_address = trouve.compiled.physical_address

        if isinstance(trouve, Trouve):
            trouve.compiled = trouve.compiled.model_copy(
                update={
                    "resolved_sql": _resolve_sql(
                        trouve.sql, reference_addresses, this_address=this_address
                    )
                }
            )
        elif isinstance(trouve, DataframeTrouve):
            # A DataFrame Trouve names each input in a list, and not in SQL.
            trouve.compiled = trouve.compiled.model_copy(
                update={
                    "input_addresses": [
                        str(reference_addresses[id(upstream)])
                        for upstream in trouve.upstream_trouves()
                    ]
                }
            )

        for test in trouve.tests:
            if isinstance(test, TestSql):
                test.resolved_sql = _resolve_sql(
                    test.sql, reference_addresses, this_address=this_address
                )
