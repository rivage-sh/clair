"""Project discovery. Clair walks the project root and compiles each Trouve file.

The module does four steps, and it names a helper module for the two steps
that hold a hard idea of their own:

1. Find the project root. ``__routing__.py`` marks it.
2. Prepare the import system, in ``core/project_imports.py``.
3. Walk the tree, import each candidate file, and take its ``trouve`` object.
4. Make the address of each Trouve, and render its SQL with
   ``core/references.py``.

This is the one function of ``core/`` that reads the file system. Each stage
after it takes objects. See ``site_docs/docs/topics/anatomy-of-a-run.md``.
"""

from __future__ import annotations

import os
from collections.abc import Sequence
from pathlib import Path
from typing import TYPE_CHECKING

import structlog

import clair as _clair_pkg

if TYPE_CHECKING:
    from clair.environments.environments import Environment

from clair.core import project_imports
from clair.core.references import (
    describe_unresolved_tokens,
    detect_imports,
    resolve_sql,
)
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


def _load_config_file(file_path: Path) -> DatabaseDefaults | SchemaDefaults | None:
    """Give the defaults of one configuration file, or None.

    A configuration file that clair cannot read is never fatal. The Trouve
    then takes the defaults of the profile.
    """
    if not file_path.exists():
        return None
    try:
        module = project_imports.load_support_file(file_path)
    except Exception as e:  # noqa: BLE001 — the user config code is unknown, but it is never fatal
        logger.debug("discovery.config_load_error", file=str(file_path), error=str(e))
        return None
    if module is None:
        return None
    defaults = getattr(module, "defaults", None)
    if isinstance(defaults, (DatabaseDefaults, SchemaDefaults)):
        return defaults
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


def _input_addresses_of(
    trouve_obj: DataframeTrouve,
    logical_addresses: dict[int, TrouveAddress],
    file_path: Path,
    own_logical_address: TrouveAddress,
) -> list[str]:
    """Give the logical address of each input, in the parameter order.

    This list is the counterpart of the addresses in the SQL of a SQL Trouve:
    discovery writes the logical address, and recompile_for_selection() changes
    it in the same way. Thus the two backends read the same tables.
    """
    input_addresses: list[str] = []
    for upstream in trouve_obj.upstream_trouves():
        dependency = logical_addresses.get(id(upstream))
        if dependency is None:
            raise DiscoveryError(
                str(file_path),
                f"the Trouve '{own_logical_address}' names an input that clair "
                "did not find. Each input must be the `trouve` object of a "
                "file in this project.",
            )
        input_addresses.append(str(dependency))
    return input_addresses


def _compile_trouve(
    trouve_obj: TrouveAbc,
    *,
    file_path: Path,
    module_name: str,
    project_root: Path,
    logical_addresses: dict[int, TrouveAddress],
    physical_addresses: dict[int, TrouveAddress],
    profile_defaults: dict[str, str | None] | None,
) -> None:
    """Give one Trouve its CompiledAttributes, and resolve the SQL of each test.

    The execution type decides the shape of the result. A SQL Trouve holds its
    addresses in its SQL, and a pandas Trouve holds them in a list.
    """
    logical = logical_addresses[id(trouve_obj)]
    physical = physical_addresses[id(trouve_obj)]
    relative_file_path = file_path.relative_to(project_root)
    config = _resolve_config(file_path, profile_defaults)

    if trouve_obj.execution_type == ExecutionType.PANDAS:
        assert isinstance(trouve_obj, DataframeTrouve)
        input_addresses = _input_addresses_of(
            trouve_obj, logical_addresses, file_path, logical
        )
        # An input that the Trouve reads two times gives one import, and a
        # Trouve that reads itself gives none.
        transform_imports = [
            address
            for address in dict.fromkeys(input_addresses)
            if address != str(logical)
        ]
        trouve_obj.compiled = CompiledAttributes(
            physical_address=physical,
            logical_address=logical,
            resolved_sql="",
            resolved_transform=trouve_obj.source_text(),
            file_path=relative_file_path,
            module_name=module_name,
            imports=transform_imports,
            input_addresses=input_addresses,
            config=config,
            execution_type=ExecutionType.PANDAS,
        )
    elif trouve_obj.execution_type == ExecutionType.SNOWFLAKE:
        assert isinstance(trouve_obj, Trouve)
        trouve_obj.compiled = CompiledAttributes(
            physical_address=physical,
            logical_address=logical,
            resolved_sql=resolve_sql(
                trouve_obj.sql, logical_addresses, this_address=logical
            ),
            file_path=relative_file_path,
            module_name=module_name,
            imports=detect_imports(trouve_obj.sql, logical_addresses, logical),
            config=config,
            execution_type=ExecutionType.SNOWFLAKE,
        )
    else:
        raise DiscoveryError(
            str(file_path),
            f"clair cannot compile the execution type "
            f"'{trouve_obj.execution_type}'.",
        )

    # A test reads the tables that its Trouve reads, thus the same map renders
    # the SQL of each test.
    for test in trouve_obj.tests:
        if isinstance(test, TestSql):
            test.resolved_sql = resolve_sql(
                test.sql, logical_addresses, this_address=logical
            )


def _collect_candidate_files(project_root: Path) -> list[Path]:
    """Give each Python file that can hold a Trouve, in path order.

    Discovery skips a directory and a file that starts with ``_``, thus a
    project holds a helper module that no Trouve declaration reaches.
    """
    candidates: list[Path] = []
    for dirpath, dirnames, filenames in os.walk(project_root):
        dirnames[:] = [
            name
            for name in dirnames
            if name not in _SKIP_DIRS and not name.startswith("_")
        ]
        for filename in filenames:
            file_path = Path(dirpath) / filename
            if _is_trouve_candidate(file_path):
                candidates.append(file_path)
    return sorted(candidates)


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

    # Empty the refs registry, and prepare the import system for this project.
    # Thus each discovery starts from a clean state, and one file below the
    # project root gives one module object.
    clear_refs()
    project_imports.make_project_importable(project_root)

    candidates = _collect_candidate_files(project_root)

    # Load each candidate. A file can be in sys.modules already, because an
    # earlier candidate imported it as a dependency.
    collected: list[tuple[TrouveAbc, TrouveAddress, Path, str]] = []
    errors: list[str] = []

    for file_path in candidates:
        module_name = str(
            file_path.relative_to(project_root).with_suffix("")
        ).replace(os.sep, ".")

        try:
            module = project_imports.load_project_file(file_path, module_name)
        except Exception as e:  # noqa: BLE001 — the user module code is unknown; clair reports the fault as an error
            logger.warning("discovery.load_error", file=str(file_path), error=str(e))
            errors.append(f"{file_path}: {e}")
            continue
        if module is None:
            continue
        # One file gives one module object, thus the name of that module wins.
        module_name = module.__name__

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
    for trouve_obj, logical_address, _, _ in collected:
        logical_addresses[id(trouve_obj)] = logical_address
        physical_addresses[id(trouve_obj)] = route(
            logical_address, trouve_obj.type, routing
        )

    # Phase B: compile each Trouve.
    # Clair puts the logical addresses in the SQL. Thus, by default, the SQL
    # reads the production upstream tables. After the selection, call
    # recompile_for_selection() to change each selected upstream address to its
    # physical address.
    for trouve_obj, _, file_path, module_name in collected:
        _compile_trouve(
            trouve_obj,
            file_path=file_path,
            module_name=module_name,
            project_root=project_root,
            logical_addresses=logical_addresses,
            physical_addresses=physical_addresses,
            profile_defaults=profile_defaults,
        )

    errors.extend(describe_unresolved_tokens(collected))

    trouve_count = len(collected)
    logger.info("discovery.complete", project_root=str(project_root), trouves=trouve_count, errors=len(errors))

    # A fault stops the run. A file that clair cannot read holds a Trouve that
    # the DAG then misses, and a run would report success after it built fewer
    # tables than the project declares.
    if errors:
        raise ProjectDiscoveryError(errors)

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
