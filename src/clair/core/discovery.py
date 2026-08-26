"""Project discovery. Clair reads the project root and loads each Trouve file."""

from __future__ import annotations

import importlib.util
import inspect
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

from clair.environments.routing import (
    RoutingEntry,
    TrouveAddress,
    detect_routing_collisions,
    route,
)
from clair.exceptions import DiscoveryError
from clair.trouves._refs import THIS_PLACEHOLDER, TROUVE_PLACEHOLDER_PREFIX
from clair.trouves._refs import clear as clear_refs
from clair.trouves.config import DatabaseDefaults, ResolvedConfig, SchemaDefaults
from clair.trouves.pandas_trouve import PandasTrouve
from clair.trouves.run_config import RunMode
from clair.trouves.test import TestSql
from clair.trouves.trouve import CompiledAttributes, ExecutionType, Trouve, TrouveAbc, TrouveType

ARTIFACTS_DIR_NAME = "_clairtifacts"
_SKIP_DIRS = {"clair", "tests", ARTIFACTS_DIR_NAME, "__pycache__", ".git", ".venv", "node_modules"}
_CONFIG_FILES = {"__database_config__.py", "__schema_config__.py"}

logger = structlog.get_logger()


def compute_logical_address(file_path: Path) -> TrouveAddress:
    """Make the logical address from the last three parts of the path.

    Example: .../database_name/schema_name/table_name.py becomes
    database_name.schema_name.table_name
    """
    return TrouveAddress.parse(".".join(file_path.with_suffix("").parts[-3:]))


def _is_trouve_candidate(file_path: Path) -> bool:
    if file_path.name.startswith("_"):
        return False
    return file_path.suffix == ".py"


def _load_config_file(
    file_path: Path, project_root: Path
) -> DatabaseDefaults | SchemaDefaults | None:
    if not file_path.exists():
        return None
    rel = file_path.relative_to(project_root).with_suffix("")
    module_name = str(rel).replace(os.sep, "_").replace(".", "_") + "_cfg"
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
    project_root: Path,
    profile_defaults: dict[str, str | None] | None = None,
) -> ResolvedConfig:
    """Make the merged config of a Trouve. The function moves up the directory tree.

    The function reads these sources in order. Each source replaces the values
    of the source before it:
    1. The profile defaults
    2. __database_config__.py
    3. __schema_config__.py
    """
    profile_wh = (profile_defaults or {}).get("warehouse")
    profile_role = (profile_defaults or {}).get("role")
    config = ResolvedConfig(
        warehouse=profile_wh if profile_wh and profile_wh.strip() else None,
        role=profile_role if profile_role and profile_role.strip() else None,
    )

    rel = file_path.relative_to(project_root)
    parts = list(rel.parts)

    if len(parts) >= 2:
        db_defaults = _load_config_file(
            project_root / parts[0] / "__database_config__.py", project_root
        )
        if isinstance(db_defaults, DatabaseDefaults):
            if db_defaults.warehouse and db_defaults.warehouse.strip():
                config.warehouse = db_defaults.warehouse
            if db_defaults.role and db_defaults.role.strip():
                config.role = db_defaults.role

    if len(parts) >= 3:
        schema_defaults = _load_config_file(
            project_root / parts[0] / parts[1] / "__schema_config__.py", project_root
        )
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
# removes the modules of the projects before it, and takes their roots off
# sys.path.
_loaded_project_roots: set[str] = set()


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

    for other_root in _loaded_project_roots - {str(project_root)}:
        if other_root in sys.path:
            sys.path.remove(other_root)
    _loaded_project_roots.clear()


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

    # Empty the refs registry and remove the modules of each project that this
    # process loaded. Thus each discovery run starts from a clean state.
    clear_refs()
    _forget_project_modules(project_root)

    # Put the project root in sys.path. Thus an import of a different Trouve works.
    project_root_str = str(project_root)
    if project_root_str not in sys.path:
        sys.path.insert(0, project_root_str)
    _loaded_project_roots.add(project_root_str)

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
        logical_address = compute_logical_address(file_path)
        module_name = str(
            file_path.relative_to(project_root).with_suffix("")
        ).replace(os.sep, ".")

        if module_name in sys.modules:
            module = sys.modules[module_name]
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

        collected.append((trouve_obj, logical_address, file_path, module_name))

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
    # With this map, clair finds the logical address of each Trouve that a PandasTrouve
    # names in its inputs.
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
            assert isinstance(trouve_obj, PandasTrouve)
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

            try:
                resolved_transform = inspect.getsource(trouve_obj.transform)
            except OSError:
                resolved_transform = repr(trouve_obj.transform)

            trouve_obj.compiled = CompiledAttributes(
                physical_address=physical,
                logical_address=logical,
                resolved_sql="",
                resolved_transform=resolved_transform,
                file_path=file_path.relative_to(project_root),
                module_name=module_name,
                imports=transform_imports,
                input_addresses=input_addresses,
                config=_resolve_config(file_path, project_root, profile_defaults),
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
                config=_resolve_config(file_path, project_root, profile_defaults),
                execution_type=ExecutionType.SNOWFLAKE,
            )
            for test in trouve_obj.tests:
                if isinstance(test, TestSql):
                    test.resolved_sql = _resolve_sql(
                        test.sql, logical_addresses, this_address=logical
                    )

    trouve_count = len(collected)
    logger.info("discovery.complete", project_root=str(project_root), trouves=trouve_count, errors=len(errors))

    return [trouve for trouve, _, _, _ in collected]


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
    input_addresses of a pandas Trouve. It changes nothing for a Trouve outside
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
        elif isinstance(trouve, PandasTrouve):
            # A pandas Trouve names each input in a list, and not in SQL.
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
