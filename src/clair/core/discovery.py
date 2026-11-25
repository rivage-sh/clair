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

import functools
import os
import threading
from collections.abc import Callable
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
    mode_renderer,
    resolve_sql,
)
from clair.environments.project_routing import ROUTING_FILE_NAME
from clair.environments.routing import (
    RoutingEntry,
    TrouveAddress,
    route,
)
from clair.exceptions import (
    DiscoveryError,
    InvalidTrouveAddressError,
    NotAProjectRootError,
    ProjectDiscoveryError,
)
from clair.trouves._refs import clear as clear_refs
from clair.trouves.config import DatabaseDefaults, ResolvedConfig, SchemaDefaults
from clair.trouves.dataframe_trouve import DataframeTrouve
from clair.trouves.run_config import RunMode
from clair.trouves.test import TestSql
from clair.trouves.trouve import (
    CompiledAttributes,
    CompiledDataframe,
    CompiledSql,
    DataframeInput,
    MaterializedTrouve,
    Source,
    SqlTrouve,
    TrouveAbc,
    table_options_of,
)

ARTIFACTS_DIR_NAME = "_clairtifacts"
DATABASE_CONFIG_FILE_NAME = "__database_config__.py"
SCHEMA_CONFIG_FILE_NAME = "__schema_config__.py"

logger = structlog.get_logger()


TROUVE_DEPTH = 3
"""The number of path parts that a Trouve file holds below its project root.

The parts are database_name/schema_name/table_name.py. A file above that depth
holds no schema name and no database name, thus clair cannot make its address.
"""


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


# The class of the `defaults` object that each configuration file gives.
_CONFIG_CLASSES: dict[str, type[DatabaseDefaults | SchemaDefaults]] = {
    DATABASE_CONFIG_FILE_NAME: DatabaseDefaults,
    SCHEMA_CONFIG_FILE_NAME: SchemaDefaults,
}

# The defaults of each configuration file, by its path.
DirectoryDefaults = dict[Path, DatabaseDefaults | SchemaDefaults]


def _config_files_above(trouve_file: Path) -> list[Path]:
    """Give the two configuration files that apply to a Trouve file, if they exist."""
    schema_directory = trouve_file.parent
    candidates = [
        schema_directory.parent / DATABASE_CONFIG_FILE_NAME,
        schema_directory / SCHEMA_CONFIG_FILE_NAME,
    ]
    return [path for path in candidates if path.is_file()]


def _load_config_files(trouve_files: list[Path]) -> tuple[DirectoryDefaults, list[str]]:
    """Run each configuration file above a Trouve one time, and give its defaults.

    A configuration file that raises, or that gives no ``defaults`` of its
    class, is a fault, in the same way as a Trouve file that raises. Each
    Trouve below it would run with the role, the warehouse and the kind of
    table of the profile, and no message would tell you.

    Returns:
        The defaults of each file, and one fault for each file that failed.
    """
    defaults_of_file: DirectoryDefaults = {}
    errors: list[str] = []
    config_files = sorted({path for trouve_file in trouve_files for path in _config_files_above(trouve_file)})
    for config_file in config_files:
        expected_class = _CONFIG_CLASSES[config_file.name]
        try:
            module = project_imports.load_support_file(config_file)
        except Exception as load_error:  # noqa: BLE001 — the user config code is unknown; clair reports the fault as an error
            errors.append(f"{config_file}: {type(load_error).__name__}: {load_error}")
            continue
        defaults = None if module is None else getattr(module, "defaults", None)
        if not isinstance(defaults, expected_class):
            errors.append(
                f"{config_file}: the file must give `defaults = {expected_class.__name__}(...)`, "
                f"but `defaults` is {type(defaults).__name__}."
            )
            continue
        defaults_of_file[config_file] = defaults
    return defaults_of_file, errors


def _resolve_config(
    file_path: Path,
    defaults_of_file: DirectoryDefaults,
    profile_defaults: dict[str, str | None] | None = None,
) -> ResolvedConfig:
    """Make the merged config of a Trouve. The function moves up the directory tree.

    The function reads these sources in order. Each source replaces the values
    of the source before it:
    1. The profile defaults
    2. __database_config__.py
    3. __schema_config__.py

    The Trouve is the last level. ``_trouve_config`` applies it.

    The function starts at the file and moves up, in the same direction as
    ``TrouveAddress.from_file_path``. The schema directory is the parent of the file,
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

    # The database file comes first, thus the schema file replaces its values.
    for config_file in _config_files_above(file_path):
        config = config.apply(defaults_of_file[config_file])
    return config


def _trouve_config(
    trouve_obj: TrouveAbc,
    file_path: Path,
    defaults_of_file: DirectoryDefaults,
    profile_defaults: dict[str, str | None] | None,
) -> ResolvedConfig:
    """Make the final config of one Trouve: the directory levels, and then the Trouve.

    Only a standard table can be transient. A View and a Source make no table,
    and Snowflake gives no TRANSIENT in the clone of an Iceberg table. Thus a
    transient default of a directory does not apply to them.
    """
    config = _resolve_config(file_path, defaults_of_file, profile_defaults).apply(trouve_obj)
    table_options = table_options_of(trouve_obj)
    if table_options is None or table_options.iceberg_settings() is not None:
        config.transient = False
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


def _merge_imports(*import_lists: list[str]) -> list[str]:
    """Give each import one time, in the order of the first occurrence."""
    return list(dict.fromkeys(address for imports in import_lists for address in imports))


# A second run of a Trouve file sets clair.run_mode, which is global, and the
# threads of a run can ask for it at one time.
_RUN_MODE_LOCK = threading.Lock()


def _tokens_in_mode_of(source: str, file_path: Path, module_name: str) -> Callable[[RunMode], str]:
    """Give the function that runs one Trouve file again, with ``clair.run_mode`` set to a mode.

    A module reads the run mode at import time, for example to add an
    incremental filter. A node whose effective mode differs from the mode of
    the import needs the SQL of its own mode: a full refresh without the
    filter, and an incremental run with it. Only that file runs again, and only
    when a node asks: each import of the file resolves to the module that
    discovery loaded.
    """

    @functools.cache
    def tokens_in_mode(run_mode: RunMode) -> str:
        with _RUN_MODE_LOCK:
            run_mode_of_the_run = _clair_pkg.run_mode
            _clair_pkg.run_mode = run_mode
            try:
                names = project_imports.run_source_again(source, file_path, module_name)
            finally:
                _clair_pkg.run_mode = run_mode_of_the_run
        rendered = names.get("trouve")
        if not isinstance(rendered, SqlTrouve):
            raise DiscoveryError(
                str(file_path), f"the file gives no SQL Trouve when clair.run_mode is {run_mode.name}."
            )
        return rendered.sql

    return tokens_in_mode


def _compile_trouve(
    trouve_obj: TrouveAbc,
    *,
    file_path: Path,
    module_name: str,
    project_root: Path,
    logical_addresses: dict[int, TrouveAddress],
    address_of_logical: dict[TrouveAddress, TrouveAddress],
    physical_addresses: dict[int, TrouveAddress],
    defaults_of_file: DirectoryDefaults,
    profile_defaults: dict[str, str | None] | None,
    run_mode: RunMode | None,
) -> None:
    """Give one Trouve its compiled attributes, and resolve the SQL of each test.

    The class of the Trouve decides the shape of the result. A SQL Trouve gets a
    CompiledSql, with its addresses in its SQL. A DataFrame Trouve gets a
    CompiledDataframe, with its addresses in a list. A Source gets the shared
    fields only.
    """
    logical = logical_addresses[id(trouve_obj)]
    physical = physical_addresses[id(trouve_obj)]
    relative_file_path = file_path.relative_to(project_root)
    config = _trouve_config(trouve_obj, file_path, defaults_of_file, profile_defaults)

    # A sample can read another Trouve, for example a seed of test users. Each
    # run keeps the edge, thus --sample does not change the DAG.
    sample_imports = (
        detect_imports(trouve_obj.sample_sql, logical_addresses, logical)
        if trouve_obj.sample_sql is not None
        else []
    )

    # A Source runs no test, thus only a Trouve that clair builds gets the
    # edges of its tests.
    test_imports = (
        _merge_imports(
            *(
                detect_imports(test.sql, logical_addresses, logical)
                for test in trouve_obj.tests
                if isinstance(test, TestSql)
            )
        )
        if isinstance(trouve_obj, MaterializedTrouve)
        else []
    )

    if isinstance(trouve_obj, DataframeTrouve):
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
        trouve_obj.compiled = CompiledDataframe(
            physical_address=physical,
            resolved_transform=trouve_obj.source_text(),
            file_path=relative_file_path,
            module_name=module_name,
            imports=_merge_imports(transform_imports, sample_imports),
            test_imports=test_imports,
            inputs=[DataframeInput(address=address) for address in input_addresses],
            config=config,
        )
    elif isinstance(trouve_obj, SqlTrouve):
        # The SQL can depend on the run mode that the module saw. Keep the text
        # of the file, and render the SQL of the other mode on demand: a node
        # whose effective mode differs from the mode of the import asks for it.
        tokens_in_mode = _tokens_in_mode_of(file_path.read_text(), file_path, module_name)
        render_in_mode = mode_renderer(tokens_in_mode, address_of_logical, logical)
        trouve_obj.compiled = CompiledSql(
            rendered_mode=run_mode or RunMode.FULL_REFRESH,
            tokens_in_mode=tokens_in_mode,
            render_in_mode=render_in_mode,
            physical_address=physical,
            resolved_sql=resolve_sql(trouve_obj.sql, logical_addresses, this_address=logical),
            file_path=relative_file_path,
            module_name=module_name,
            imports=_merge_imports(
                detect_imports(trouve_obj.sql, logical_addresses, logical), sample_imports
            ),
            test_imports=test_imports,
            config=config,
        )
    elif isinstance(trouve_obj, Source):
        # A Source holds no SQL, thus only its sample can read another Trouve.
        trouve_obj.compiled = CompiledAttributes(
            physical_address=physical,
            file_path=relative_file_path,
            module_name=module_name,
            imports=sample_imports,
            config=config,
        )
    else:
        raise DiscoveryError(
            str(file_path),
            f"clair cannot compile the Trouve class {type(trouve_obj).__name__}.",
        )

    # A test reads the tables that its Trouve reads, thus the same map renders
    # the SQL of each test. THIS stays a marker: the plan decides the object
    # that the test reads, and two Trouves can share one TestSql object.
    for test in trouve_obj.tests:
        if isinstance(test, TestSql):
            test.resolved_sql = resolve_sql(test.sql, logical_addresses, this_address=None)


def _is_read_by_discovery(directory: Path) -> bool:
    """Tell if discovery reads the files of a directory.

    Discovery skips a name that starts with ``.``, such as ``.git`` or
    ``.venv``, and a name that starts with ``_``, which is a helper that you
    import yourself. It also skips each virtual environment, whatever its
    name, for example ``venv.nosync``: a virtual environment holds thousands of
    modules that discovery must not import. ``pyvenv.cfg`` marks one (PEP 405).

    Each other directory is read, thus a database directory with a name that
    Snowflake cannot use stops the run, and does not disappear.
    """
    if directory.name.startswith((".", "_")):
        return False
    return not (directory / "pyvenv.cfg").is_file()


def _collect_candidate_files(project_root: Path) -> list[Path]:
    """Give each Python file that can hold a Trouve, in path order.

    Discovery skips a directory and a file that starts with ``_``, thus a
    project holds a helper module that no Trouve declaration reaches.
    """
    candidates: list[Path] = []
    for dirpath, dirnames, filenames in os.walk(project_root):
        dirnames[:] = [name for name in dirnames if _is_read_by_discovery(Path(dirpath) / name)]
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
    # ``import clair; clair.env.connection.role``.
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

        # A name that makes no address is one fault of many, thus clair
        # validate reports each of them together.
        try:
            logical_address = TrouveAddress.from_file_path(file_path)
        except InvalidTrouveAddressError as address_error:
            errors.append(f"{file_path}: {address_error}")
            continue

        collected.append((trouve_obj, logical_address, file_path, module_name))

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

    # A token without an address stops the run here, before the compiler reads
    # it. Thus the fault names the file that ran two times.
    errors.extend(describe_unresolved_tokens(collected, logical_addresses))

    defaults_of_file, config_errors = _load_config_files(
        [file_path for _, _, file_path, _ in collected]
    )
    errors.extend(config_errors)

    # A fault stops the run. A file that clair cannot read holds a Trouve that
    # the DAG then misses, and a run would report success after it built fewer
    # tables than the project declares.
    if errors:
        logger.info("discovery.complete", project_root=str(project_root), trouves=len(collected), errors=len(errors))
        raise ProjectDiscoveryError(errors)

    # Phase B: compile each Trouve.
    # Clair puts the logical addresses in the SQL. Thus, by default, the SQL
    # reads the production upstream tables. After the selection, call
    # recompile_for_selection() to change each selected upstream address to its
    # physical address.
    # Discovery renders the SQL of another run mode with the logical addresses
    # too. Each Trouve shares one map, thus discovery stays linear in the
    # number of Trouves.
    address_of_logical = {address: address for address in logical_addresses.values()}
    for trouve_obj, _, file_path, module_name in collected:
        _compile_trouve(
            trouve_obj,
            file_path=file_path,
            module_name=module_name,
            project_root=project_root,
            logical_addresses=logical_addresses,
            address_of_logical=address_of_logical,
            physical_addresses=physical_addresses,
            defaults_of_file=defaults_of_file,
            profile_defaults=profile_defaults,
            run_mode=run_mode,
        )

    logger.info("discovery.complete", project_root=str(project_root), trouves=len(collected), errors=0)

    return [trouve for trouve, _, _, _ in collected]
