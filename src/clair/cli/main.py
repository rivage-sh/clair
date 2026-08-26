"""The Clair CLI. This module is the click entry point."""

from __future__ import annotations

import os
import re
import shutil
import sys
from collections import Counter
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import click
import structlog
import uuid6

from clair._logging import configure_logging
from clair.adapters.base import WarehouseAdapter
from clair.adapters.snowflake import SnowflakeAdapter
from clair.core.compiler import CompiledNodeInfo, write_compile_output
from clair.core.dag import build_dag
from clair.core.dag_render import render_dag
from clair.core.discovery import (
    ARTIFACTS_DIR_NAME,
    discover_project,
    find_routing_collisions,
    recompile_for_selection,
)
from clair.core.runner import RunStatus, run_project
from clair.core.scaffold import scaffold_project, write_environments_yml
from clair.core.selector import expand_selectors
from clair.core.test_runner import format_test_output, run_tests
from clair.core.text_references import find_text_references
from clair.docs.catalog import build_catalog
from clair.docs.server import serve
from clair.environments.environments import DEFAULT_THREADS, load_environment
from clair.environments.project_routing import (
    ROUTING_FILE_NAME,
    ProjectRouting,
    load_project_routing,
)
from clair.environments.routing import (
    collect_routing_problems,
    describe_routing,
    detect_routing_collisions,
    route,
)
from clair.exceptions import (
    ClairError,
    CompileError,
    EnvironmentsFileNotFoundError,
    InvalidRoutingConfigError,
    InvalidTrouveAddressError,
)
from clair.trouves.address import TrouveAddress
from clair.trouves.run_config import RunMode
from clair.trouves.trouve import ExecutionType, TrouveType

logger = structlog.get_logger()


@click.group()
@click.version_option(version="0.1.0", prog_name="clair")
def cli() -> None:
    """Clair -- Python-native data transformation for Snowflake."""
    configure_logging()


@cli.command()
@click.option(
    "--project",
    default=None,
    type=click.Path(file_okay=False),
    help="Directory for the new Clair project (default: the current directory)",
)
def init(project: str | None) -> None:
    """Create a new Clair project with example Trouves and configuration."""
    # Step 1 -- The project directory.
    if project is None:
        project = click.prompt("Project directory", default=".", type=str)
    project_dir = Path(project).resolve()

    # Step 2 -- The environment.
    environments_path = Path.home() / ".clair" / "environments.yml"
    environments_existed = environments_path.exists()
    skip_environments_in_scaffold = False

    if environments_existed:
        click.echo("  ~/.clair/environments.yml exists. Clair keeps it.")
        skip_environments_in_scaffold = True
    else:
        skip_environments_in_scaffold = True
        _prompt_and_write_environment()

    # Step 3 -- The source table.
    source_full_table_name: str = click.prompt(
        "Give an example Snowflake table that contains source data (for example source.orders.raw)",
        default="source",
        type=str,
    )
    source_full_table_name_split = source_full_table_name.split('.')
    if len(source_full_table_name_split) != 3:
        click.echo("Error: Give a full table name in the format database.schema.table (for example source.orders.raw)", err=True)
        sys.exit(1)
    source_database_name, source_schema_name, source_table_name = source_full_table_name_split

    # Step 4 -- The scaffold files.
    results = scaffold_project(
        project_dir,
        source_database_name=source_database_name,
        source_schema_name=source_schema_name,
        source_table_name=source_table_name,
        # The code above wrote the profiles, or found them. Give a home_dir
        # that lets the scaffold find that file and report "skipped". The real
        # home directory holds the real file.
    )

    click.echo("")
    for status, filepath in results:
        # Hide the environments.yml line. The code above found that file, or
        # the user gave the values for it.
        if skip_environments_in_scaffold and filepath == str(environments_path):
            continue
        click.echo(f"  {status}  {filepath}")
    click.echo("")

    # Step 5 -- The .gitignore file.
    gitignore_path = project_dir / ".gitignore"
    gitignore_path.write_text(f"/{ARTIFACTS_DIR_NAME}\n")
    click.echo(f"  created  {gitignore_path}")
    click.echo("")

    # Step 6 -- The next steps for the user.
    click.echo("\u2713 Project ready.")
    click.echo("")
    click.echo("Next steps:")
    click.echo(f"  1. clair compile --project {project_dir}")
    click.echo(f"  2. clair run    --project {project_dir}")
    click.echo("")


def _resolve_project_routing(project_root: Path, env_name: str) -> ProjectRouting:
    """Load the project routing entry and warn about an absent entry.

    A routing file that does not name the active environment is almost always a
    typo. Passthrough routing then writes to the production names, so clair
    tells the user before any SQL runs.
    """
    project_routing = load_project_routing(project_root, env_name)

    if project_routing.is_unnamed_environment:
        click.echo(
            click.style(
                f"\nWarning: {ROUTING_FILE_NAME} does not name the environment "
                f"'{env_name}'.",
                fg="yellow",
                bold=True,
            )
        )
        click.echo(
            f"  Trouves write to their logical (production) addresses.\n"
            f"  The file names: {', '.join(project_routing.environment_names) or 'nothing'}\n"
        )

    return project_routing


def _print_routing_collision_warnings(trouves: list, env_name: str, routing) -> None:
    """Show a clear warning about each routing collision, before the SQL starts."""
    collisions = find_routing_collisions(trouves)
    if not collisions:
        return

    n = len(collisions)
    header = "1 routing collision" if n == 1 else f"{n} routing collisions"
    if routing is not None:
        header += f" (env: {env_name}, entry: {describe_routing(routing)})"
    else:
        header += f" (env: {env_name})"

    click.echo(click.style(f"\nWarning: Clair found {header}", fg="yellow", bold=True))

    for physical_address, logical_sources in collisions:
        click.echo(f"\n  {physical_address}")
        for source in logical_sources:
            click.echo(f"    ↳ {source}")

    click.echo(
        f"\n  Fix: give one Trouve a different name, change the routing entry in "
        f"{ROUTING_FILE_NAME},\n  or use --select to remove one Trouve from this run.\n"
    )


def _prompt_and_write_environment() -> None:
    """Ask the user for the Snowflake connection data and write environments.yml."""

    def _hint(sql: str) -> None:
        click.echo(f"  hint: select {sql};", err=True)

    def _require(prompt_text: str, **kwargs) -> str:
        while True:
            value = click.prompt(prompt_text, **kwargs)
            if str(value).strip():
                return str(value).strip()
            click.echo(f"You must give a value for {prompt_text}.")

    click.echo("")
    env_name = click.prompt("Environment name", default="dev", type=str)

    click.echo("")
    _hint("concat(current_organization_name(), '-', current_account_name()) as account")
    account = _require("Snowflake account (e.g. myorg-myaccount)")

    click.echo("")
    _hint("current_user() as user")
    user = _require("Snowflake user")

    click.echo("")
    click.echo("Authentication method:")
    click.echo("  1. Private key")
    click.echo("  2. Password")
    click.echo("  3. SSO (externalbrowser)")
    auth_choice = click.prompt("Enter choice", default="1", type=str)

    env_data: dict[str, Any] = {
        "account": account,
        "user": user,
    }

    if auth_choice == "1":
        private_key_path = _require("Private key path")
        env_data["private_key_path"] = private_key_path
        key_encrypted = click.confirm("Is the key encrypted?", default=False)
        if key_encrypted:
            passphrase = click.prompt(
                "Private key passphrase", hide_input=True, type=str
            )
            env_data["private_key_passphrase"] = passphrase
    elif auth_choice == "2":
        password = click.prompt("Password", hide_input=True, type=str)
        env_data["password"] = password
    elif auth_choice == "3":
        env_data["authenticator"] = "externalbrowser"

    click.echo("")
    _hint("current_warehouse() as warehouse")
    warehouse = _require("Warehouse")
    env_data["warehouse"] = warehouse

    click.echo("")
    role = click.prompt("Role (leave empty to use the default role of the user)", default="", type=str, show_default=False)
    if role:
        env_data["role"] = role

    click.echo("")
    _hint("current_region() as region")
    region = _require("Region (e.g. us-east-1)")
    env_data["region"] = region

    click.echo("")
    _hint("current_account() as account_locator")
    account_locator = _require("Account locator (e.g. abc12345)")
    env_data["account_locator"] = account_locator

    click.echo("")
    threads = click.prompt(
        "Trouves that run at one time (threads)",
        default=DEFAULT_THREADS,
        type=click.IntRange(min=1),
    )
    env_data["threads"] = threads

    click.echo("")
    write_environments_yml(env_data, env_name=env_name)


@cli.command(name="compile")
@click.option(
    "--select",
    multiple=True,
    help="Pattern that selects Trouves. You can use globs and + operators. Example: --select='+mydb.analytics.orders' --select='mydb.reports.*'",
)
@click.option(
    "--exclude",
    multiple=True,
    help="Pattern that removes Trouves. The syntax is the same as --select. Clair applies it after the selection.",
)
@click.option(
    "--project",
    default=".",
    type=click.Path(exists=True, file_okay=False),
    help="Path to the Clair project root",
)
@click.option(
    "--env",
    default=None,
    help="Environment name from ~/.clair/environments.yml",
)
@click.option(
    "--run-mode",
    type=click.Choice(["full_refresh", "incremental"], case_sensitive=False),
    default="full_refresh",
    help="Run mode. full_refresh writes all tables again. incremental writes only the new data.",
)
def compile_cmd(select: tuple[str, ...], exclude: tuple[str, ...], project: str, env: str | None, run_mode: str) -> None:
    """Compile the project and show the new SQL. This needs no Snowflake connection."""
    project_root = Path(project).resolve()
    run_mode_enum = RunMode(run_mode)
    run_id = uuid6.uuid7().hex

    environment = None
    env_name = env or "dev"
    try:
        env_name, environment = load_environment(env)
    except EnvironmentsFileNotFoundError:
        logger.warning("compile.no_environments_file", detail="Clair compiles without an environment. Run `clair init` to make environments.yml.")
    except ClairError as e:
        logger.error("compile.error", error=str(e))
        sys.exit(1)

    try:
        routing = _resolve_project_routing(project_root, env_name).entry
    except ClairError as e:
        logger.error("compile.error", error=str(e))
        sys.exit(1)

    try:
        discovered = discover_project(project_root, routing=routing, environment=environment, run_mode=run_mode_enum)
        _print_routing_collision_warnings(discovered, env_name, routing)
        dag = build_dag(discovered)

        expanded = expand_selectors(dag, select if select else None)
        selected = [n for n in expanded if dag.get_trouve(n).type != TrouveType.SOURCE]
        if exclude:
            excluded_set = set(expand_selectors(dag, exclude))
            selected = [n for n in selected if n not in excluded_set]
        recompile_for_selection(discovered, set(selected))

        source_count = sum(1 for n in dag.nodes if dag.get_trouve(n).type == TrouveType.SOURCE)
        trouve_count = len(dag.nodes) - source_count

        logger.info("compile.start", run_id=run_id, project=str(project_root), trouves=trouve_count, sources=source_count, run_mode=run_mode)

        artifacts_dir = project_root / ARTIFACTS_DIR_NAME / run_id

        def _on_node_compiled(node_info: CompiledNodeInfo) -> None:
            parts = node_info.name.split(".")
            extension = None
            if node_info.execution_type == ExecutionType.PANDAS:
                extension = ".py"
            elif node_info.execution_type == ExecutionType.SNOWFLAKE:
                extension = ".sql"
            else:
                raise CompileError(f"Unknown execution_type '{node_info.execution_type}' for {node_info.name}")
            artifact_file = artifacts_dir / "/".join(parts[:-1]) / f"{parts[-1]}{extension}"
            logger.info("compile.node", trouve=node_info.name, dependencies=node_info.dependencies, artifact_file=str(artifact_file))

        write_compile_output(dag, selected, project_root, on_node_compiled=_on_node_compiled, run_mode=run_mode_enum, run_id=run_id, use_staging=True)
        logger.info("compile.complete", run_id=run_id, artifacts_dir=str(artifacts_dir))

    except (InvalidRoutingConfigError, InvalidTrouveAddressError) as e:
        logger.error("compile.routing_error", error=str(e))
        click.echo("\n  Run `clair validate` to see every routing problem.\n", err=True)
        sys.exit(1)
    except ClairError as e:
        logger.error("compile.error", error=str(e))
        sys.exit(1)


@cli.command()
@click.option(
    "--project",
    default=".",
    help="Path to the Clair project root (defaults to current directory)",
)
@click.option(
    "--env",
    default=None,
    help="Environment name to route for; matches an entry in __routing__.py",
)
def validate(project: str, env: str | None) -> None:
    """Apply the project routing entries to every Trouve.

    This command needs no Snowflake credentials, so CI runs it on every change.
    """
    project_root = Path(project).resolve()
    env_name = env or os.environ.get("CLAIR_ENV") or "dev"

    try:
        project_routing = _resolve_project_routing(project_root, env_name)
        # Find the Trouves with routing off. A bad entry then reports as a
        # routing problem, and does not stop discovery at the first Trouve.
        discovered = discover_project(project_root, routing=None)
    except ClairError as e:
        logger.error("validate.error", error=str(e))
        sys.exit(1)

    routing = project_routing.entry
    # Keep the (logical address, type) pair, and not the Trouve. The address is
    # the only part that the collision report needs, and here it is never None.
    routable: list[tuple[TrouveAddress, TrouveType]] = [
        (trouve.compiled.logical_address, trouve.type)
        for trouve in discovered
        if trouve.compiled is not None
    ]

    click.echo(f"\n  environment: {env_name}")
    click.echo(f"  routing file: {project_routing.file_path or 'none'}")
    click.echo(f"  entry: {describe_routing(routing)}")
    click.echo(f"  Trouves to route: {len(routable)}\n")

    problems = collect_routing_problems(discovered, routing)
    for logical_address, problem in problems:
        click.echo(click.style(f"  ✗ {logical_address}", fg="red", bold=True))
        click.echo(f"    {problem}\n")

    collisions: list[tuple[str, list[str]]] = []
    if not problems:
        logical_to_physical = {
            str(logical_address): str(route(logical_address, trouve_type, routing))
            for logical_address, trouve_type in routable
        }
        collisions = detect_routing_collisions(logical_to_physical)
        for physical_address, logical_sources in collisions:
            click.echo(click.style(f"  ✗ {physical_address}", fg="red", bold=True))
            click.echo("    Two or more Trouves route to this one target:")
            for source in logical_sources:
                click.echo(f"      ↳ {source}")
            click.echo("")

    # An address that an author writes as text makes no DAG edge, and routing
    # does not move it. Both faults are silent at run time.
    text_references = find_text_references(discovered)
    for reference in text_references:
        click.echo(click.style(f"  ✗ {reference.logical_address}", fg="red", bold=True))
        click.echo(
            f"    The {reference.location} names '{reference.text_address}' as text."
        )
        click.echo(
            "    Import that Trouve and put it in an f-string. Clair then makes a DAG\n"
            "    edge, and the routing entry moves the address.\n"
        )

    problem_count = len(problems) + len(collisions) + len(text_references)
    if problem_count:
        label = "problem" if problem_count == 1 else "problems"
        click.echo(click.style(f"  {problem_count} {label} found.\n", fg="red", bold=True))
        sys.exit(1)

    click.echo(
        click.style(
            "  ✓ Every physical address is valid. No collisions. Each reference is a Trouve.\n",
            fg="green",
        )
    )


@cli.command()
@click.option(
    "--select",
    multiple=True,
    help="Glob pattern that selects Trouves. Give the option again to add more patterns. Example: --select='mydb.analytics.*' --select='mydb.reports.*'",
)
@click.option(
    "--project",
    default=".",
    type=click.Path(exists=True, file_okay=False),
    help="Path to the Clair project root",
)
def dag(select: tuple[str, ...], project: str) -> None:
    """Show the project DAG as a tree with indents."""
    project_root = Path(project).resolve()

    try:
        discovered = discover_project(project_root)
        dag_graph = build_dag(discovered)

        selected = list(select) if select else None
        output = render_dag(dag_graph, selected)
        click.echo(output.render())

    except ClairError as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.option(
    "--project",
    default=".",
    type=click.Path(exists=True, file_okay=False),
    help="Path to the Clair project root",
)
@click.option(
    "--port",
    default=8741,
    type=int,
    help="Port for the local docs server",
)
@click.option(
    "--host",
    default="127.0.0.1",
    help="Address for the local docs server",
)
@click.option(
    "--no-browser",
    is_flag=True,
    help="Do not open the browser",
)
def docs(project: str, port: int, host: str, no_browser: bool) -> None:
    """Start a local web UI. It shows the project documentation and the lineage."""
    project_root = Path(project).resolve()

    try:
        discovered = discover_project(project_root)
        dag = build_dag(discovered)

        catalog = build_catalog(dag, project_root)

        source_count = sum(1 for t in dag.trouves if t.type == TrouveType.SOURCE)
        trouve_count = len(dag.nodes) - source_count

        logger.info("docs.start", project=str(project_root), trouves=trouve_count, sources=source_count)

        serve(catalog, host=host, port=port, open_browser=not no_browser)

    except OSError as e:
        if "Address already in use" in str(e) or "address already in use" in str(e):
            logger.error("docs.port_in_use", port=port, detail=f"Port {port} is in use. Use --port with a different number.")
        else:
            logger.error("docs.error", error=str(e))
        sys.exit(1)
    except ClairError as e:
        logger.error("docs.error", error=str(e))
        sys.exit(1)


@cli.command()
@click.option(
    "--select",
    multiple=True,
    help="Pattern that selects Trouves. You can use globs and + operators. Example: --select='+mydb.analytics.orders' --select='mydb.reports.*'",
)
@click.option(
    "--exclude",
    multiple=True,
    help="Pattern that removes Trouves. The syntax is the same as --select. Clair applies it after the selection.",
)
@click.option(
    "--project",
    default=".",
    type=click.Path(exists=True, file_okay=False),
    help="Path to the Clair project root",
)
@click.option(
    "--env",
    default=None,
    help="Environment name from ~/.clair/environments.yml",
)
@click.option(
    "--run-mode",
    type=click.Choice(["full_refresh", "incremental"], case_sensitive=False),
    default="full_refresh",
    help="Run mode. full_refresh writes all tables again. incremental writes only the new data.",
)
@click.option(
    "--no-test",
    is_flag=True,
    default=False,
    help="Do not run the data quality tests after a successful run.",
)
@click.option(
    "--sample",
    is_flag=True,
    default=False,
    help="Run the tests on a sample of each Trouve. Clair does not run the row count tests.",
)
@click.option(
    "--threads",
    type=click.IntRange(min=1),
    default=None,
    help="The number of Trouves that run at one time. The default comes from the environment.",
)
def run(select: tuple[str, ...], exclude: tuple[str, ...], project: str, env: str | None, run_mode: str, no_test: bool, sample: bool, threads: int | None) -> None:
    """Run the Trouves on Snowflake. Then run the data quality tests."""
    project_root = Path(project).resolve()
    run_mode_enum = RunMode(run_mode)
    run_id = uuid6.uuid7().hex

    # Clair always writes through a staging address: build there, test there, and
    # promote after the tests pass. The tests give the guarantee, so --no-test
    # stops the staging step. Nothing then decides the promotion, and a staging
    # address gives only cost.
    use_staging = not no_test
    if no_test:
        logger.warning(
            "run.staging_disabled",
            reason="--no-test skips the tests that decide the promotion, so clair writes to each physical address directly",
        )

    try:
        # Load the environment.
        env_name, environment = load_environment(env)

        # The command line replaces the thread count of the environment.
        thread_count = threads if threads is not None else environment.threads

        # Find the Trouves and make the DAG.
        profile_defaults = {
            "warehouse": environment.warehouse,
            "role": environment.role,
        }
        routing = _resolve_project_routing(project_root, env_name).entry
        discovered = discover_project(project_root, profile_defaults, routing=routing, environment=environment, run_mode=run_mode_enum)
        _print_routing_collision_warnings(discovered, env_name, routing)
        dag = build_dag(discovered)

        # Keep only the Trouves that the selector gives.
        expanded = expand_selectors(dag, select if select else None)
        selected = [n for n in expanded if dag.get_trouve(n).type != TrouveType.SOURCE]
        if exclude:
            excluded_set = set(expand_selectors(dag, exclude))
            selected = [n for n in selected if n not in excluded_set]

        if not selected:
            click.echo("Clair found no Trouves to run.")
            return

        recompile_for_selection(discovered, set(selected))
        write_compile_output(dag, selected, project_root, run_mode=run_mode_enum, run_id=run_id, use_staging=use_staging)

        # If account_locator is absent, tell the user. The query URLs stay empty.
        if not environment.account_locator:
            logger.warning("run.no_account_locator", env=env_name, detail="Clair cannot show the query URLs.")

        # Connect and run. Show the result of each node immediately.
        adapter = SnowflakeAdapter()
        adapter.connect(environment.to_connection_dict())

        test_failures: list[str] = []

        def on_node_success(
            node_name: str, query_address: str, node_adapter: WarehouseAdapter
        ) -> bool:
            # node_adapter is the connection of the thread that ran the node.
            # A parallel run must not send these test queries on a different
            # connection, because that connection holds a different context.
            node_test_results = run_tests(
                dag, [node_name], node_adapter,
                use_sample=sample,
                query_addresses={node_name: query_address},
            )
            passed = all(r.passed for r in node_test_results)
            if not passed:
                # list.append is atomic, thus the threads need no lock here.
                test_failures.append(node_name)
            return passed

        try:
            total = len(selected)
            # A connection that no Trouve uses gives nothing. A connection
            # costs one login, not credits: Snowflake bills the warehouse per
            # second while it runs, and an idle session starts no warehouse.
            thread_count = min(thread_count, total)
            logger.info("run.start", run_id=run_id, env=env_name, project=str(project_root), trouves=total, run_mode=run_mode, use_staging=use_staging, threads=thread_count)

            results = list(run_project(
                dag, selected, adapter,
                run_mode=run_mode_enum,
                run_id=run_id,
                after_node_success=on_node_success if not no_test else None,
                use_staging=use_staging,
                threads=thread_count,
            ))

            counts = Counter(r.status for r in results)
            logger.info("run.complete", run_id=run_id, succeeded=counts[RunStatus.SUCCESS], failed=counts[RunStatus.FAILURE], skipped=counts[RunStatus.SKIPPED])

            if counts[RunStatus.FAILURE] or test_failures:
                sys.exit(1)

        finally:
            adapter.close()

    except (InvalidRoutingConfigError, InvalidTrouveAddressError) as e:
        logger.error("run.routing_error", error=str(e))
        click.echo("\n  Run `clair validate` to see every routing problem.\n", err=True)
        sys.exit(1)
    except ClairError as e:
        logger.error("run.error", error=str(e))
        sys.exit(1)


@cli.command()
@click.option(
    "--select",
    multiple=True,
    help="Pattern that selects Trouves. You can use globs and + operators. Example: --select='+mydb.analytics.orders' --select='mydb.reports.*'",
)
@click.option(
    "--exclude",
    multiple=True,
    help="Pattern that removes Trouves. The syntax is the same as --select. Clair applies it after the selection.",
)
@click.option(
    "--project",
    default=".",
    type=click.Path(exists=True, file_okay=False),
    help="Path to the Clair project root",
)
@click.option(
    "--env",
    default=None,
    help="Environment name from ~/.clair/environments.yml",
)
@click.option(
    "--sample",
    is_flag=True,
    default=False,
    help="Run the tests on a sample of each Trouve. Clair does not run the row count tests.",
)
@click.option(
    "--threads",
    type=click.IntRange(min=1),
    default=None,
    help="The number of Trouves that clair tests at one time. The default comes from the environment.",
)
def test(
    select: tuple[str, ...], exclude: tuple[str, ...], project: str, env: str | None, sample: bool, threads: int | None
) -> None:
    """Run the data quality tests on Snowflake."""
    project_root = Path(project).resolve()

    try:
        # Load the environment.
        env_name, environment = load_environment(env)

        # The command line replaces the thread count of the environment.
        thread_count = threads if threads is not None else environment.threads

        # Find the Trouves and make the DAG.
        profile_defaults = {
            "warehouse": environment.warehouse,
            "role": environment.role,
        }
        routing = _resolve_project_routing(project_root, env_name).entry
        discovered = discover_project(project_root, profile_defaults, routing=routing, environment=environment)
        dag = build_dag(discovered)

        # Keep the Trouves that the selector gives. Keep each SOURCE too, so
        # that the selector can match a SOURCE. run_tests skips each SOURCE.
        selected = expand_selectors(dag, select if select else None)
        if exclude:
            excluded_set = set(expand_selectors(dag, exclude))
            selected = [n for n in selected if n not in excluded_set]

        if not selected:
            logger.info("test.no_trouves_selected")
            return

        # Connect and run the tests.
        adapter = SnowflakeAdapter()
        adapter.connect(environment.to_connection_dict())

        try:
            # A connection that no Trouve uses gives nothing. A connection
            # costs one login, not credits.
            thread_count = min(thread_count, len(selected))
            logger.info("test.start", project=str(project_root), trouves=len(selected), threads=thread_count)
            results = run_tests(dag, selected, adapter, use_sample=sample, threads=thread_count)

            if not results:
                logger.info("test.no_tests_found")
                return

            output = format_test_output(results)
            logger.info("test.complete", passed=output.passed_count, failed=output.failed_count, errors=output.error_count)

            # If one test failed, or one test caused an error, stop with an error.
            if any(not r.passed for r in results):
                sys.exit(1)
        finally:
            adapter.close()

    except ClairError as e:
        logger.error("test.error", error=str(e))
        sys.exit(1)


def _parse_before_spec(spec: str) -> datetime:
    """Read a --before age and give the equivalent UTC limit.

    The function accepts these forms:
        - Usual words: 'today', 'yesterday', 'last_week'
        - A time span: '7d', '24h', '30m'
        - An ISO date or time: '2026-03-01', '2026-03-01T12:00:00'
    """
    now = datetime.now(tz=UTC)
    # A calendar limit starts at local midnight. The code then changes the time
    # to UTC. Thus "today" is today in the time zone of the user, not in UTC.
    local_today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0).astimezone(UTC)

    if spec == "today":
        return local_today
    if spec == "yesterday":
        return local_today - timedelta(days=1)
    if spec == "last_week":
        # The Monday of the week before, in local time, changed to UTC.
        this_monday = local_today - timedelta(days=local_today.astimezone().weekday())
        return this_monday - timedelta(weeks=1)

    m = re.match(r"^(\d+)([dhm])$", spec)
    if m:
        n, unit = int(m.group(1)), m.group(2)
        delta = {"d": timedelta(days=n), "h": timedelta(hours=n), "m": timedelta(minutes=n)}[unit]
        return now - delta
    try:
        dt = datetime.fromisoformat(spec)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        return dt
    except ValueError:
        raise click.BadParameter(
            f"Clair cannot read '{spec}'. Use 'today', 'yesterday', 'last_week', a duration such as '7d' or '24h', or an ISO date such as '2026-03-01'.",
            param_hint="--before",
        )


def _run_id_to_time(run_id: str) -> datetime | None:
    """Read the UTC creation time from a UUIDv7 hex run_id.

    A UUIDv7 holds the Unix time in milliseconds in the first 48 bits. Those
    bits are the first 12 hex characters. The function gives None if the run_id
    is not a hex string of 32 characters.
    """
    if len(run_id) != 32 or not all(c in "0123456789abcdef" for c in run_id):
        return None
    ts_ms = int(run_id[:12], 16)
    return datetime.fromtimestamp(ts_ms / 1000, tz=UTC)


@cli.command()
@click.option(
    "--project",
    default=".",
    type=click.Path(exists=True, file_okay=False),
    help="Path to the Clair project root",
)
@click.option(
    "--before",
    default=None,
    metavar="AGE",
    help="Remove the artifacts that are older than AGE. AGE can be 'today', 'yesterday', 'last_week', a lookback such as '7d' or '24h', or an ISO date such as '2026-03-01'.",
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Show the artifacts to remove, but do not remove them.",
)
@click.option(
    "--yes",
    is_flag=True,
    help="Do not ask for confirmation.",
)
def clean(project: str, before: str | None, dry_run: bool, yes: bool) -> None:
    """Delete the compiled artifacts in _clairtifacts/."""
    project_root = Path(project).resolve()
    artifacts_root = project_root / ARTIFACTS_DIR_NAME

    if not artifacts_root.exists():
        click.echo(f"Clair found no {ARTIFACTS_DIR_NAME}/ directory. There is nothing to remove.")
        return

    cutoff: datetime | None = None
    if before is not None:
        cutoff = _parse_before_spec(before)

    # Collect the run directories to delete.
    to_remove: list[Path] = []
    for entry in sorted(artifacts_root.iterdir()):
        if not entry.is_dir():
            continue
        if cutoff is not None:
            created = _run_id_to_time(entry.name)
            if created is None or created >= cutoff:
                continue
        to_remove.append(entry)

    if not to_remove:
        click.echo("There is nothing to remove.")
        return

    click.echo(f"{'Clair will remove' if dry_run else 'Clair removes'} {len(to_remove)} artifact run(s):")
    for path in to_remove:
        ts = _run_id_to_time(path.name)
        ts_str = ts.strftime("%Y-%m-%d %H:%M:%S UTC") if ts else "unknown time"
        click.echo(f"  {path.name}  ({ts_str})")

    if dry_run:
        return

    if not yes:
        click.confirm(f"\nRemove {len(to_remove)} run(s)?", abort=True)

    for path in to_remove:
        shutil.rmtree(path)

    click.echo(f"Clair removed {len(to_remove)} run(s).")


if __name__ == "__main__":
    cli()
