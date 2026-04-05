"""Project discovery -- walk the project root, find and load Trouve files."""

from __future__ import annotations

import importlib.util
import inspect
import os
import re
import sys
from pathlib import Path
from typing import TYPE_CHECKING

import clair as _clair_pkg
import structlog

if TYPE_CHECKING:
    from clair.environments.environments import Environment

from clair.environments.routing import RoutingConfig, detect_routing_collisions, route
from clair.trouves._refs import THIS_PLACEHOLDER, TROUVE_PLACEHOLDER_PREFIX
from clair.trouves._refs import clear as clear_refs
from clair.trouves.run_config import RunMode
from clair.trouves.config import DatabaseDefaults, ResolvedConfig, SchemaDefaults
from clair.trouves.test import TestSql
from clair.trouves.trouve import CompiledAttributes, ExecutionType, Trouve, TrouveType


ARTIFACTS_DIR_NAME = "_clairtifacts"
_SKIP_DIRS = {"clair", "tests", ARTIFACTS_DIR_NAME, "__pycache__", ".git", ".venv", "node_modules"}
_CONFIG_FILES = {"__database_config__.py", "__schema_config__.py"}

logger = structlog.get_logger()


def compute_full_name(file_path: Path) -> str:
    """Derive the fully-qualified Snowflake name from the last three path components.

    Example: .../database_name/schema_name/table_name.py -> database_name.schema_name.table_name
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
    except Exception:
        pass
    return None


def _resolve_config(
    file_path: Path,
    project_root: Path,
    profile_defaults: dict[str, str | None] | None = None,
) -> ResolvedConfig:
    """Build merged config for a Trouve by walking up its directory tree.

    Resolution order (later overrides earlier):
    1. Profile defaults
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
    """Replace placeholder tokens with real full_names.

    Resolves both cross-Trouve placeholders (``__CLAIR_TROUVE_<id>__``) and the
    THIS sentinel (``__CLAIR_THIS__``) to ``this_name``, which is the logical name
    of the Trouve being compiled. ``recompile_for_selection`` later upgrades any
    logical names to routed names for selected upstreams.
    """
    def replace(m: re.Match[str]) -> str:
        return id_to_full_name.get(int(m.group(1)), m.group(0))
    result = _PLACEHOLDER_RE.sub(replace, sql)
    return result.replace(THIS_PLACEHOLDER, this_name)


def _detect_imports(
    sql: str, id_to_full_name: dict[int, str], own_full_name: str
) -> list[str]:
    """Extract full_names of other Trouves referenced as placeholders in the SQL."""
    imports = []
    for obj_id_str in _PLACEHOLDER_RE.findall(sql):
        dep_name = id_to_full_name.get(int(obj_id_str))
        if dep_name and dep_name != own_full_name and dep_name not in imports:
            imports.append(dep_name)
    return imports


def discover_project(
    project_root: Path,
    profile_defaults: dict[str, str | None] | None = None,
    routing: RoutingConfig | None = None,
    environment: "Environment | None" = None,
    run_mode: RunMode | None = None,
) -> list[Trouve]:
    """Discover all Trouves in a project.

    Walks the project root, loads all Trouve files, resolves SQL placeholders,
    detects import relationships, and returns compiled Trouve objects.

    Args:
        project_root: Absolute path to the project root directory.
        profile_defaults: Default warehouse/role from the active profile.
        routing: Routing configuration for physical name overrides.
        environment: Active environment. Exposed as ``clair.env`` so Trouve
            modules can read it during loading (e.g. for feature flags).
        run_mode: Requested run mode (FULL_REFRESH or INCREMENTAL). Exposed as
            ``clair.run_mode`` so Trouve modules can read it during loading
            (e.g. to make WHERE clauses conditional on run mode).

    Returns:
        List of Trouve objects, each with .compiled set.
    """
    project_root = project_root.resolve()

    # Expose the active environment and run mode on the clair package so Trouve
    # modules can read them during loading (e.g. ``import clair; clair.env.role``).
    _clair_pkg.env = environment
    _clair_pkg.run_mode = run_mode

    # Clear refs registry and purge previously loaded project modules so each
    # discovery run starts from a clean slate (important for tests and repeated calls).
    clear_refs()
    for mod_name in list(sys.modules.keys()):
        mod_file = getattr(sys.modules[mod_name], "__file__", None)
        if mod_file:
            try:
                Path(mod_file).relative_to(project_root)
                del sys.modules[mod_name]
            except ValueError:
                pass

    # Add project root to sys.path so cross-Trouve imports resolve normally.
    project_root_str = str(project_root)
    if project_root_str not in sys.path:
        sys.path.insert(0, project_root_str)

    # Collect candidate files.
    candidates: list[Path] = []
    for dirpath, dirnames, filenames in os.walk(project_root):
        dirnames[:] = [d for d in dirnames if d not in _SKIP_DIRS and not d.startswith("_")]
        for filename in filenames:
            file_path = Path(dirpath) / filename
            if _is_trouve_candidate(file_path):
                candidates.append(file_path)
    candidates.sort()

    # Load each candidate. A file may already be in sys.modules if it was
    # imported as a dependency of an earlier candidate.
    collected: list[tuple[Trouve, str, Path, str]] = []
    errors: list[str] = []

    for file_path in candidates:
        full_name = compute_full_name(file_path)
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
            except Exception as e:
                logger.warning("discovery.load_error", file=str(file_path), error=str(e))
                errors.append(f"{file_path}: {e}")
                continue

        trouve_obj = getattr(module, "trouve", None)
        if not isinstance(trouve_obj, Trouve):
            continue

        collected.append((trouve_obj, full_name, file_path, module_name))

    # Phase A: compute logical names and routed names for all Trouves.
    # logical_name = filesystem-derived name (used for DAG edges, selectors)
    # routed_name  = physical target name (used in SQL and DDL)
    # SOURCE Trouves always pass through routing unchanged.
    logical_names: dict[int, str] = {}
    routed_names: dict[int, str] = {}
    collision_check: dict[str, str] = {}

    for trouve_obj, full_name, _, _ in collected:
        logical_names[id(trouve_obj)] = full_name
        routed = route(full_name, trouve_obj.type, routing)
        routed_names[id(trouve_obj)] = routed
        if trouve_obj.type != TrouveType.SOURCE:
            collision_check[full_name.upper()] = routed

    # Build an id-to-logical-name map for df_fn dependency resolution.
    # This lets us look up logical names for Trouve objects referenced as df_fn parameter defaults.
    id_to_logical_name: dict[int, str] = {
        id(trouve_obj): logical_names[id(trouve_obj)]
        for trouve_obj, _, _, _ in collected
    }

    # Phase B: compile each Trouve.
    # SQL is resolved with logical names so it reads from production upstreams by default.
    # Call recompile_for_selection() after filtering to upgrade references for selected
    # upstreams to their routed names.
    for trouve_obj, full_name, file_path, module_name in collected:
        logical = logical_names[id(trouve_obj)]
        routed = routed_names[id(trouve_obj)]

        if trouve_obj.df_fn is not None:
            df_imports = []
            for param in inspect.signature(trouve_obj.df_fn).parameters.values():
                if isinstance(param.default, Trouve):
                    dep_logical = id_to_logical_name.get(id(param.default))
                    if dep_logical and dep_logical != logical and dep_logical not in df_imports:
                        df_imports.append(dep_logical)

            try:
                resolved_df_fn = inspect.getsource(trouve_obj.df_fn)
            except OSError:
                resolved_df_fn = repr(trouve_obj.df_fn)

            trouve_obj.compiled = CompiledAttributes(
                full_name=routed,
                logical_name=logical,
                resolved_sql="",
                resolved_df_fn=resolved_df_fn,
                file_path=file_path.relative_to(project_root),
                module_name=module_name,
                imports=df_imports,
                config=_resolve_config(file_path, project_root, profile_defaults),
                execution_type=ExecutionType.PANDAS,
            )
            for test in trouve_obj.tests:
                if isinstance(test, TestSql):
                    test.sql = _resolve_sql(test.sql, logical_names, this_name=logical)
        else:
            trouve_obj.compiled = CompiledAttributes(
                full_name=routed,
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


def find_routing_collisions(trouves: list[Trouve]) -> list[tuple[str, list[str]]]:
    """Return (routed_target, [logical_sources]) for any routing collisions.

    A collision occurs when two non-SOURCE Trouves are routed to the same physical target.
    Call this after discover_project() to surface collisions for display.

    Returns an empty list when no routing is active (logical == routed for all Trouves).
    """
    logical_to_routed = {
        trouve.compiled.logical_name: trouve.compiled.full_name
        for trouve in trouves
        if trouve.compiled and trouve.type != TrouveType.SOURCE
    }
    return detect_routing_collisions(logical_to_routed)


def recompile_for_selection(trouves: list[Trouve], selected_names: set[str]) -> None:
    """Upgrade SQL references for selected upstreams from logical to routed names.

    After discover_project(), each Trouve's resolved_sql uses logical (production) names
    for all upstream references. This function upgrades references to TABLE/VIEW upstreams
    that are in the selected set to their routed names, because those Trouves will be
    materialized at the routed location during this run.

    SOURCE upstreams and non-selected TABLE/VIEW upstreams keep their logical names,
    so partial runs still read from the correct production tables.

    Mutates Trouves in-place. No-op when no routing is active (logical == routed).

    Args:
        trouves: All Trouves returned by discover_project().
        selected_names: Routed full_names of Trouves selected for this run
            (as returned by the DAG selector — these are the physical write targets).
    """
    # Build a mapping of logical → routed for selected non-SOURCE Trouves that are actually rerouted.
    logical_to_routed: dict[str, str] = {}
    for t in trouves:
        if (
            t.compiled
            and t.compiled.full_name in selected_names
            and t.type != TrouveType.SOURCE
            and t.compiled.full_name != t.compiled.logical_name
        ):
            logical_to_routed[t.compiled.logical_name] = t.compiled.full_name

    if not logical_to_routed:
        return

    # For each selected Trouve, replace logical upstream names with routed names in the SQL.
    # Use negative lookaround so we don't accidentally match a longer identifier that starts
    # with the same prefix (e.g. "db.s.foo" inside "db.s.foobar" would not match).
    for t in trouves:
        if not t.compiled or t.compiled.full_name not in selected_names:
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
