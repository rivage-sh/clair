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

from clair.environments.routing import RoutingEntry, detect_routing_collisions, route
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


def compute_full_name(file_path: Path) -> str:
    """Make the full Snowflake name from the last three parts of the path.

    Example: .../database_name/schema_name/table_name.py becomes
    database_name.schema_name.table_name
    """
    return ".".join(file_path.with_suffix("").parts[-3:])


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


def _resolve_sql(sql: str, id_to_full_name: dict[int, str], this_name: str) -> str:
    """Replace each placeholder token with the true physical_name.

    The function replaces a token that points to a different Trouve
    (``__CLAIR_TROUVE_<id>__``). It also replaces the THIS marker
    (``__CLAIR_THIS__``) with ``this_name``, the logical name of the current
    Trouve. Later, ``recompile_for_selection`` changes a logical name to a
    routed name for each selected upstream Trouve.
    """
    def replace(m: re.Match[str]) -> str:
        return id_to_full_name.get(int(m.group(1)), m.group(0))
    result = _PLACEHOLDER_RE.sub(replace, sql)
    return result.replace(THIS_PLACEHOLDER, this_name)


def _detect_imports(
    sql: str, id_to_full_name: dict[int, str], own_full_name: str
) -> list[str]:
    """Give the physical_name of each other Trouve that the SQL points to with a token."""
    imports = []
    for obj_id_str in _PLACEHOLDER_RE.findall(sql):
        dep_name = id_to_full_name.get(int(obj_id_str))
        if dep_name and dep_name != own_full_name and dep_name not in imports:
            imports.append(dep_name)
    return imports


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
        routing: The routing entry for the physical names, from __routing__.py.
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

    # Empty the refs registry and remove each project module from a previous
    # run. Thus each discovery run starts from a clean state. This is important
    # for the tests and for more than one call in one process.
    clear_refs()
    for mod_name in list(sys.modules.keys()):
        mod_file = getattr(sys.modules[mod_name], "__file__", None)
        if mod_file:
            try:
                Path(mod_file).relative_to(project_root)
                del sys.modules[mod_name]
            except ValueError:
                pass

    # Put the project root in sys.path. Thus an import of a different Trouve works.
    project_root_str = str(project_root)
    if project_root_str not in sys.path:
        sys.path.insert(0, project_root_str)

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
    collected: list[tuple[TrouveAbc, str, Path, str]] = []
    errors: list[str] = []

    for file_path in candidates:
        physical_name = compute_full_name(file_path)
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

        collected.append((trouve_obj, physical_name, file_path, module_name))

    # Phase A: make the logical name and the routed name of each Trouve.
    # logical_name = the name from the file path. DAG edges and selectors use it.
    # routed_name  = the physical target name. The SQL and the DDL use it.
    # The routing entry sees every Trouve, and a SOURCE Trouve is not an
    # exception. An entry that must keep a SOURCE at its logical name examines
    # the Trouve type.
    logical_names: dict[int, str] = {}
    routed_names: dict[int, str] = {}
    collision_check: dict[str, str] = {}

    for trouve_obj, physical_name, _, _ in collected:
        logical_names[id(trouve_obj)] = physical_name
        routed = route(physical_name, trouve_obj.type, routing)
        routed_names[id(trouve_obj)] = routed
        # A SOURCE is in the collision check too. A TABLE that routes to the
        # address of a SOURCE replaces the data that the SOURCE reads.
        collision_check[physical_name.upper()] = routed

    # Make a map from an id to a logical name, for the pandas dependencies. With
    # this map, clair finds the logical name of each Trouve that a PandasTrouve
    # names in its inputs.
    id_to_logical_name: dict[int, str] = {
        id(trouve_obj): logical_names[id(trouve_obj)]
        for trouve_obj, _, _, _ in collected
    }

    # Phase B: compile each Trouve.
    # Clair puts the logical names in the SQL. Thus, by default, the SQL reads
    # the production upstream tables. After the selection, call
    # recompile_for_selection() to change each selected upstream name to its
    # routed name.
    for trouve_obj, physical_name, file_path, module_name in collected:
        logical = logical_names[id(trouve_obj)]
        routed = routed_names[id(trouve_obj)]

        if trouve_obj.execution_type == ExecutionType.PANDAS:
            assert isinstance(trouve_obj, PandasTrouve)
            transform_imports = []
            for upstream in trouve_obj.upstream_trouves():
                dep_logical = id_to_logical_name.get(id(upstream))
                if dep_logical and dep_logical != logical and dep_logical not in transform_imports:
                    transform_imports.append(dep_logical)

            try:
                resolved_transform = inspect.getsource(trouve_obj.transform)
            except OSError:
                resolved_transform = repr(trouve_obj.transform)

            trouve_obj.compiled = CompiledAttributes(
                physical_name=routed,
                logical_name=logical,
                resolved_sql="",
                resolved_transform=resolved_transform,
                file_path=file_path.relative_to(project_root),
                module_name=module_name,
                imports=transform_imports,
                config=_resolve_config(file_path, project_root, profile_defaults),
                execution_type=ExecutionType.PANDAS,
            )
            for test in trouve_obj.tests:
                if isinstance(test, TestSql):
                    test.sql = _resolve_sql(test.sql, logical_names, this_name=logical)
        else:
            assert isinstance(trouve_obj, Trouve)
            trouve_obj.compiled = CompiledAttributes(
                physical_name=routed,
                logical_name=logical,
                resolved_sql=_resolve_sql(trouve_obj.sql, logical_names, this_name=logical),
                file_path=file_path.relative_to(project_root),
                module_name=module_name,
                imports=_detect_imports(trouve_obj.sql, logical_names, logical),
                config=_resolve_config(file_path, project_root, profile_defaults),
                execution_type=ExecutionType.SNOWFLAKE,
            )
            for test in trouve_obj.tests:
                if isinstance(test, TestSql):
                    test.sql = _resolve_sql(test.sql, logical_names, this_name=logical)

    trouve_count = len(collected)
    logger.info("discovery.complete", project_root=str(project_root), trouves=trouve_count, errors=len(errors))

    return [trouve for trouve, _, _, _ in collected]


def find_routing_collisions(trouves: Sequence[TrouveAbc]) -> list[tuple[str, list[str]]]:
    """Give a (routed_target, [logical_sources]) pair for each routing collision.

    A collision occurs when two Trouves that are not SOURCE Trouves route to one
    physical target. Call this function after discover_project(), to show each
    collision to the user.

    The result is an empty list when no routing policy is active. Then the
    logical name and the routed name are equal for each Trouve.
    """
    logical_to_routed = {
        trouve.compiled.logical_name: trouve.compiled.physical_name
        for trouve in trouves
        if trouve.compiled and trouve.type != TrouveType.SOURCE
    }
    return detect_routing_collisions(logical_to_routed)


def recompile_for_selection(trouves: Sequence[TrouveAbc], selected_names: set[str]) -> None:
    """Change each selected upstream name in the SQL from logical to routed.

    After discover_project(), the resolved_sql of each Trouve holds the logical
    production name of each upstream Trouve. This function changes the name of
    each selected TABLE or VIEW upstream Trouve to its routed name, because
    clair materializes that Trouve at the routed location in this run.

    A SOURCE upstream Trouve keeps its logical name. A TABLE or VIEW upstream
    Trouve that the user did not select also keeps its logical name. Thus a
    partial run reads the correct production tables.

    This function changes each Trouve in place. It does nothing when no routing
    policy is active, because the logical name and the routed name are equal.

    Args:
        trouves: Each Trouve from discover_project().
        selected_names: The routed full_names of the Trouves for this run. The
            DAG selector gives them. Each name is a physical write target.
    """
    # Map the logical name to the routed name for each selected Trouve that is
    # not a SOURCE and that has a different routed name.
    logical_to_routed: dict[str, str] = {}
    for t in trouves:
        if (
            t.compiled
            and t.compiled.physical_name in selected_names
            and t.type != TrouveType.SOURCE
            and t.compiled.physical_name != t.compiled.logical_name
        ):
            logical_to_routed[t.compiled.logical_name] = t.compiled.physical_name

    if not logical_to_routed:
        return

    # In the SQL of each selected Trouve, replace the logical upstream name with
    # the routed name. The pattern has a negative lookaround. Thus a longer
    # identifier with the same prefix stays as it is. For example, the pattern
    # "db.s.foo" does not match the text "db.s.foobar".
    for t in trouves:
        if not t.compiled or t.compiled.physical_name not in selected_names:
            continue

        sql = t.compiled.resolved_sql
        for logical, routed in logical_to_routed.items():
            pattern = r"(?<![A-Za-z0-9_.\\])" + re.escape(logical) + r"(?![A-Za-z0-9_.])"
            sql = re.sub(pattern, routed, sql, flags=re.IGNORECASE)

        t.compiled = t.compiled.model_copy(update={"resolved_sql": sql})

        for test in t.tests:
            if isinstance(test, TestSql):
                test_sql = test.sql
                for logical, routed in logical_to_routed.items():
                    pattern = r"(?<![A-Za-z0-9_.\\])" + re.escape(logical) + r"(?![A-Za-z0-9_.])"
                    test_sql = re.sub(pattern, routed, test_sql, flags=re.IGNORECASE)
                test.sql = test_sql
