"""Clair CLI -- click entrypoint."""

from __future__ import annotations

import re
import shutil
import sys
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path

import click
import structlog
import uuid6

from clair._logging import configure_logging
from clair.adapters.snowflake import SnowflakeAdapter
from clair.environments.environments import load_environment
from clair.core.compiler import CompiledNodeInfo, write_compile_output
from clair.trouves.trouve import ExecutionType
from clair.core.dag import build_dag
from clair.core.dag_render import render_dag
from clair.environments.routing import DatabaseOverrideRouting, SchemaIsolationRouting
from clair.core.discovery import ARTIFACTS_DIR_NAME, discover_project, find_routing_collisions, recompile_for_selection
from clair.core.runner import RunStatus, run_project
from clair.core.scaffold import scaffold_project, write_environments_yml
from clair.core.selector import expand_selectors
from clair.core.test_runner import format_test_output, run_tests
from clair.docs.catalog import build_catalog
from clair.docs.server import serve
from clair.exceptions import ClairError, CompileError, EnvironmentsFileNotFoundError
from clair.trouves.run_config import RunMode
from clair.trouves.trouve import TrouveType


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
    help="Directory to initialise as a Clair project (default: cwd)",
)
def init(project: str | None) -> None:
    """Create a new Clair project with example Trouves and config."""
    # Step 1 -- Project directory
    if project is None:
        project = click.prompt("Project directory", default=".", type=str)
    project_dir = Path(project).resolve()

    # Step 2 -- Environment setup
    environments_path = Path.home() / ".clair" / "environments.yml"
    environments_existed = environments_path.exists()
    skip_environments_in_scaffold = False

    if environments_existed:
        click.echo("  ~/.clair/environments.yml already exists, skipping.")
        skip_environments_in_scaffold = True
    else:
        skip_environments_in_scaffold = True
        _prompt_and_write_environment()

    # Step 3 -- Source table
    source_full_table_name: str = click.prompt(
        "What is an example Snowflake table that contains source data? (eg source.orders.raw)",
        default="source",
        type=str,
    )
    source_full_table_name_split = source_full_table_name.split('.')
    if len(source_full_table_name_split) != 3:
        click.echo("Error: Please provide a fully qualified table name in the format database.schema.table (e.g. source.orders.raw)", err=True)
        sys.exit(1)
    source_database_name, source_schema_name, source_table_name = source_full_table_name_split

    # Step 4 -- Scaffold files
    results = scaffold_project(
        project_dir,
        source_database_name=source_database_name,
        source_schema_name=source_schema_name,
        source_table_name=source_table_name,
        # If we already handled profiles (existed or created interactively),
        # pass a home_dir that ensures scaffold sees the existing file and
        # reports "skipped". We use the real home so it finds the real file.
    )

    click.echo("")
    for status, filepath in results:
        # Suppress the environments.yml line if we already handled it above
        # (either it pre-existed or the user filled it in interactively).
        if skip_environments_in_scaffold and filepath == str(environments_path):
            continue
        click.echo(f"  {status}  {filepath}")
    click.echo("")

    # Step 5 -- .gitignore
    gitignore_path = project_dir / ".gitignore"
    gitignore_path.write_text(f"/{ARTIFACTS_DIR_NAME}\n")
    click.echo(f"  created  {gitignore_path}")
    click.echo("")

    # Step 6 -- Next steps
    click.echo("\u2713 Project ready.")
    click.echo("")
    click.echo("Next steps:")
    click.echo(f"  1. clair compile --project {project_dir}")
    click.echo(f"  2. clair run    --project {project_dir}")
    click.echo("")


def _print_routing_collision_warnings(trouves: list, env_name: str, routing) -> None:
    """Print a prominent warning block for any routing collisions, before SQL runs."""
    collisions = find_routing_collisions(trouves)
    if not collisions:
        return

    policy_desc = ""
    if routing is not None:
        if isinstance(routing, DatabaseOverrideRouting):
            policy_desc = f"database_override → {routing.database_name}"
        elif isinstance(routing, SchemaIsolationRouting):
            policy_desc = f"schema_isolation → {routing.database_name}.{routing.schema_name}"

    n = len(collisions)
    header = f"{'collision' if n == 1 else f'{n} collisions'} detected"
    if policy_desc:
        header += f" (env: {env_name}, policy: {policy_desc})"
    else:
        header += f" (env: {env_name})"

    click.echo(click.style(f"\nWarning: routing {header}", fg="yellow", bold=True))

    for routed_target, logical_sources in collisions:
        click.echo(f"\n  {routed_target}")
        for source in logical_sources:
            click.echo(f"    ↳ {source}")

    click.echo(
        "\n  Fix: rename a colliding Trouve, adjust the routing policy in "
        "environments.yml,\n  or use --select to exclude one from this run.\n"
    )


def _prompt_and_write_environment() -> None:
    """Interactively collect Snowflake connection details and write environments.yml."""

    def _hint(sql: str) -> None:
        click.echo(f"  hint: select {sql};", err=True)

    def _require(prompt_text: str, **kwargs) -> str:
        while True:
            value = click.prompt(prompt_text, **kwargs)
            if str(value).strip():
                return str(value).strip()
            click.echo(f"{prompt_text} is required.")

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

    env_data: dict[str, str] = {
        "account": account,
        "user": user,
    }

    if auth_choice == "1":
        private_key_path = _require("Private key path")
        env_data["private_key_path"] = private_key_path
        key_encrypted = click.confirm("Key is encrypted?", default=False)
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
    role = click.prompt("Role (leave blank to use user default)", default="", type=str, show_default=False)
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
    write_environments_yml(env_data, env_name=env_name)


@cli.command(name="compile")
@click.option(
    "--select",
    multiple=True,
    help="Selector pattern to filter Trouves; supports globs and + operators (e.g., --select='+mydb.analytics.orders' --select='mydb.reports.*')",
)
@click.option(
    "--exclude",
    multiple=True,
    help="Selector pattern to exclude Trouves; same syntax as --select, applied after selection.",
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
    help="Run mode: full_refresh recreates all tables; incremental applies only new data.",
)
def compile_cmd(select: tuple[str, ...], exclude: tuple[str, ...], project: str, env: str | None, run_mode: str) -> None:
    """Compile the project and show generated SQL (no Snowflake connection)."""
    project_root = Path(project).resolve()
    run_mode_enum = RunMode(run_mode)
    run_id = uuid6.uuid7().hex

    routing = None
    environment = None
    env_name = env or "dev"
    try:
        env_name, environment = load_environment(env)
        routing = environment.routing
    except EnvironmentsFileNotFoundError:
        logger.warning("compile.no_environments_file", detail="compiling without routing; run `clair init` to create environments.yml")
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

        write_compile_output(dag, selected, project_root, on_node_compiled=_on_node_compiled, run_mode=run_mode_enum, run_id=run_id)
        logger.info("compile.complete", run_id=run_id, artifacts_dir=str(artifacts_dir))

    except ClairError as e:
        logger.error("compile.error", error=str(e))
        sys.exit(1)


@cli.command()
@click.option(
    "--select",
    multiple=True,
    help="Glob pattern to filter Trouves; repeat to union patterns (e.g., --select='mydb.analytics.*' --select='mydb.reports.*')",
)
@click.option(
    "--project",
    default=".",
    type=click.Path(exists=True, file_okay=False),
    help="Path to the Clair project root",
)
def dag(select: tuple[str, ...], project: str) -> None:
    """Show the project DAG as an indented tree."""
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
    help="Bind address for the local docs server",
)
@click.option(
    "--no-browser",
    is_flag=True,
    help="Do not open the browser automatically",
)
def docs(project: str, port: int, host: str, no_browser: bool) -> None:
    """Start a local web UI showing project documentation and lineage."""
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
            logger.error("docs.port_in_use", port=port, detail=f"Port {port} is already in use. Try --port <other>")
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
    help="Selector pattern to filter Trouves; supports globs and + operators (e.g., --select='+mydb.analytics.orders' --select='mydb.reports.*')",
)
@click.option(
    "--exclude",
    multiple=True,
    help="Selector pattern to exclude Trouves; same syntax as --select, applied after selection.",
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
    help="Run mode: full_refresh recreates all tables; incremental applies only new data.",
)
@click.option(
    "--no-test",
    is_flag=True,
    default=False,
    help="Skip running data quality tests after a successful run.",
)
@click.option(
    "--sample",
    is_flag=True,
    default=False,
    help="Run post-run tests against a sample of each Trouve (skips row count tests).",
)
def run(select: tuple[str, ...], exclude: tuple[str, ...], project: str, env: str | None, run_mode: str, no_test: bool, sample: bool) -> None:
    """Run Trouves against Snowflake, then run data quality tests."""
    project_root = Path(project).resolve()
    run_mode_enum = RunMode(run_mode)
    run_id = uuid6.uuid7().hex

    try:
        # Load environment
        env_name, environment = load_environment(env)

        # Discover and build DAG
        profile_defaults = {
            "warehouse": environment.warehouse,
            "role": environment.role,
        }
        discovered = discover_project(project_root, profile_defaults, routing=environment.routing, environment=environment, run_mode=run_mode_enum)
        _print_routing_collision_warnings(discovered, env_name, environment.routing)
        dag = build_dag(discovered)

        # Filter by selector
        expanded = expand_selectors(dag, select if select else None)
        selected = [n for n in expanded if dag.get_trouve(n).type != TrouveType.SOURCE]
        if exclude:
            excluded_set = set(expand_selectors(dag, exclude))
            selected = [n for n in selected if n not in excluded_set]

        if not selected:
            click.echo("No Trouves selected to run.")
            return

        recompile_for_selection(discovered, set(selected))
        write_compile_output(dag, selected, project_root, run_mode=run_mode_enum, run_id=run_id)

        # Warn if account_locator is missing (query URLs will be incomplete)
        if not environment.account_locator:
            logger.warning("run.no_account_locator", env=env_name, detail="query URLs will not be available")

        # Connect and run, streaming each node result as it completes
        adapter = SnowflakeAdapter()
        adapter.connect(environment.to_connection_dict())

        test_failures: list[str] = []

        def on_node_success(node_name: str) -> bool:
            node_test_results = run_tests(dag, [node_name], adapter, use_sample=sample)
            passed = all(r.passed for r in node_test_results)
            if not passed:
                test_failures.append(node_name)
            return passed

        try:
            total = len(selected)
            logger.info("run.start", run_id=run_id, env=env_name, project=str(project_root), trouves=total, run_mode=run_mode)

            results = list(run_project(
                dag, selected, adapter,
                run_mode=run_mode_enum,
                run_id=run_id,
                after_node_success=on_node_success if not no_test else None,
            ))

            counts = Counter(r.status for r in results)
            logger.info("run.complete", run_id=run_id, succeeded=counts[RunStatus.SUCCESS], failed=counts[RunStatus.FAILURE], skipped=counts[RunStatus.SKIPPED])

            if counts[RunStatus.FAILURE] or test_failures:
                sys.exit(1)

        finally:
            adapter.close()

    except ClairError as e:
        logger.error("run.error", error=str(e))
        sys.exit(1)


@cli.command()
@click.option(
    "--select",
    multiple=True,
    help="Selector pattern to filter Trouves; supports globs and + operators (e.g., --select='+mydb.analytics.orders' --select='mydb.reports.*')",
)
@click.option(
    "--exclude",
    multiple=True,
    help="Selector pattern to exclude Trouves; same syntax as --select, applied after selection.",
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
    help="Run tests against a sample of each Trouve (skips row count tests).",
)
def test(
    select: tuple[str, ...], exclude: tuple[str, ...], project: str, env: str | None, sample: bool
) -> None:
    """Run data quality tests against Snowflake."""
    project_root = Path(project).resolve()

    try:
        # Load environment
        _, environment = load_environment(env)

        # Discover and build DAG
        profile_defaults = {
            "warehouse": environment.warehouse,
            "role": environment.role,
        }
        discovered = discover_project(project_root, profile_defaults, routing=environment.routing, environment=environment)
        dag = build_dag(discovered)

        # Filter by selector -- include all nodes (even SOURCEs) so that
        # the selector can match them; run_tests skips SOURCEs internally.
        selected = expand_selectors(dag, select if select else None)
        if exclude:
            excluded_set = set(expand_selectors(dag, exclude))
            selected = [n for n in selected if n not in excluded_set]

        if not selected:
            logger.info("test.no_trouves_selected")
            return

        # Connect and run tests
        adapter = SnowflakeAdapter()
        adapter.connect(environment.to_connection_dict())

        try:
            logger.info("test.start", project=str(project_root), trouves=len(selected))
            results = run_tests(dag, selected, adapter, use_sample=sample)

            if not results:
                logger.info("test.no_tests_found")
                return

            output = format_test_output(results)
            logger.info("test.complete", passed=output.passed_count, failed=output.failed_count, errors=output.error_count)

            # Exit with error if any failures or errors
            if any(not r.passed for r in results):
                sys.exit(1)
        finally:
            adapter.close()

    except ClairError as e:
        logger.error("test.error", error=str(e))
        sys.exit(1)


def _parse_before_spec(spec: str) -> datetime:
    """Parse a --before age into a UTC datetime cutoff.

    Accepts:
        - Natural language: 'today', 'yesterday', 'last_week'
        - Duration lookbacks: '7d', '24h', '30m'
        - ISO date/datetime strings: '2026-03-01', '2026-03-01T12:00:00'
    """
    now = datetime.now(tz=timezone.utc)
    # Calendar boundaries use local midnight, then convert to UTC so that
    # e.g. "today" means today in the user's timezone, not UTC.
    local_today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0).astimezone(timezone.utc)

    if spec == "today":
        return local_today
    if spec == "yesterday":
        return local_today - timedelta(days=1)
    if spec == "last_week":
        # Monday of last calendar week (local time, converted to UTC)
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
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except ValueError:
        raise click.BadParameter(
            f"Cannot parse '{spec}'. Use 'today', 'yesterday', 'last_week', a duration like '7d'/'24h', or an ISO date like '2026-03-01'.",
            param_hint="--before",
        )


def _run_id_to_time(run_id: str) -> datetime | None:
    """Extract the UTC creation time from a UUIDv7 hex run_id.

    UUIDv7 encodes Unix timestamp in ms in the first 48 bits (12 hex chars).
    Returns None if run_id is not a valid 32-char hex string.
    """
    if len(run_id) != 32 or not all(c in "0123456789abcdef" for c in run_id):
        return None
    ts_ms = int(run_id[:12], 16)
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)


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
    help="Remove artifacts older than AGE. Accepts 'today', 'yesterday', 'last_week', lookbacks like '7d'/'24h', or ISO dates like '2026-03-01'.",
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Show what would be deleted without deleting anything.",
)
@click.option(
    "--yes",
    is_flag=True,
    help="Skip confirmation prompt.",
)
def clean(project: str, before: str | None, dry_run: bool, yes: bool) -> None:
    """Remove compiled artifacts from _clairtifacts/."""
    project_root = Path(project).resolve()
    artifacts_root = project_root / ARTIFACTS_DIR_NAME

    if not artifacts_root.exists():
        click.echo(f"No {ARTIFACTS_DIR_NAME}/ directory found — nothing to clean.")
        return

    cutoff: datetime | None = None
    if before is not None:
        cutoff = _parse_before_spec(before)

    # Collect run directories to remove
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
        click.echo("Nothing to clean.")
        return

    click.echo(f"{'Would remove' if dry_run else 'Removing'} {len(to_remove)} artifact run(s):")
    for path in to_remove:
        ts = _run_id_to_time(path.name)
        ts_str = ts.strftime("%Y-%m-%d %H:%M:%S UTC") if ts else "unknown time"
        click.echo(f"  {path.name}  ({ts_str})")

    if dry_run:
        return

    if not yes:
        click.confirm(f"\nDelete {len(to_remove)} run(s)?", abort=True)

    for path in to_remove:
        shutil.rmtree(path)

    click.echo(f"Removed {len(to_remove)} run(s).")


if __name__ == "__main__":
    cli()
