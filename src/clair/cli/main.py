"""The Clair CLI. This module is the click entry point."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import click
import structlog

from clair import api as clair_api
from clair._logging import configure_logging
from clair.core.artifacts import InvalidBeforeSpecError
from clair.core.dag import build_dag
from clair.core.dag_render import render_dag
from clair.core.discovery import (
    ARTIFACTS_DIR_NAME,
    discover_project,
    find_project_root,
)
from clair.core.scaffold import scaffold_project, write_environments_yml
from clair.environments.environments import (
    DEFAULT_THREADS,
    Environment,
    load_environment,
    resolve_env_name,
)
from clair.environments.project_routing import ROUTING_FILE_NAME
from clair.exceptions import (
    ClairError,
    EnvironmentsFileNotFoundError,
    InvalidRoutingConfigError,
    InvalidTrouveAddressError,
    ProjectRootNotFoundError,
)
from clair.trouves.run_config import RunMode

logger = structlog.get_logger()


def _load_environment(env_name: str | None) -> Environment:
    """Read ~/.clair/environments.yml, and give the environment of *env_name*.

    The CLI reads the file. The Python API functions accept the Environment
    object, thus a caller that holds the settings writes no file.
    """
    return load_environment(env_name)[1]


def _load_environment_or_name(env_name: str | None) -> Environment | str:
    """Give the Environment, or the name only if environments.yml is absent.

    `clair compile` and `clair validate` make no connection, thus they continue
    without the file. The name still selects the routing entry.
    """
    try:
        return _load_environment(env_name)
    except EnvironmentsFileNotFoundError:
        logger.warning(
            "cli.no_environments_file",
            detail="Clair continues without an environment. Run `clair init` to make environments.yml.",
        )
        return resolve_env_name(env_name)


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

    # A project inside a project gives two roots, and the root search would
    # then find the outer one. Clair refuses, and it names the file it found.
    try:
        existing_root = find_project_root(project_dir)
    except ProjectRootNotFoundError:
        existing_root = None
    if existing_root is not None:
        click.echo(
            f"Error: {existing_root / ROUTING_FILE_NAME} exists, thus "
            f"{project_dir} is inside a clair project. Give --project a "
            "directory outside that project.",
            err=True,
        )
        sys.exit(1)

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
    click.echo(f"  1. cd {project_dir}")
    click.echo("  2. clair compile")
    click.echo("  3. clair run")
    click.echo("")


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
def compile_cmd(select: tuple[str, ...], exclude: tuple[str, ...], env: str | None, run_mode: str) -> None:
    """Compile the project and show the new SQL. This needs no Snowflake connection."""
    try:
        clair_api.compile(
            select=select,
            exclude=exclude,
            env=_load_environment_or_name(env),
            run_mode=RunMode(run_mode),
        )
    except (InvalidRoutingConfigError, InvalidTrouveAddressError) as e:
        logger.error("compile.routing_error", error=str(e))
        click.echo("\n  Run `clair validate` to see every routing problem.\n", err=True)
        sys.exit(1)
    except ClairError as e:
        logger.error("compile.error", error=str(e))
        sys.exit(1)


@cli.command()
@click.option(
    "--env",
    default=None,
    help="Environment name to route for; matches an entry in __routing__.py",
)
def validate(env: str | None) -> None:
    """Apply the project routing entries to every Trouve.

    This command needs no Snowflake credentials, so CI runs it on every change.
    """
    try:
        report = clair_api.validate(env=_load_environment_or_name(env))
    except ClairError as e:
        logger.error("validate.error", error=str(e))
        sys.exit(1)

    colour = "green" if report.is_valid else "red"
    click.echo(click.style(report.render(), fg=colour))
    if not report.is_valid:
        sys.exit(1)


@cli.command()
@click.option(
    "--select",
    multiple=True,
    help="Glob pattern that selects Trouves. Give the option again to add more patterns. Example: --select='mydb.analytics.*' --select='mydb.reports.*'",
)
def dag(select: tuple[str, ...]) -> None:
    """Show the project DAG as a tree with indents."""
    try:
        project_root = find_project_root(Path.cwd())
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
def docs(port: int, host: str, no_browser: bool) -> None:
    """Start a local web UI. It shows the project documentation and the lineage."""
    try:
        clair_api.docs(host=host, port=port, open_browser=not no_browser)
    except OSError as e:
        if "address already in use" in str(e).lower():
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
def run(select: tuple[str, ...], exclude: tuple[str, ...], env: str | None, run_mode: str, no_test: bool, sample: bool, threads: int | None) -> None:
    """Run the Trouves on Snowflake. Then run the data quality tests."""
    try:
        summary = clair_api.run(
            select=select,
            exclude=exclude,
            env=_load_environment(env),
            run_mode=RunMode(run_mode),
            test=not no_test,
            sample=sample,
            threads=threads,
        )
    except (InvalidRoutingConfigError, InvalidTrouveAddressError) as e:
        logger.error("run.routing_error", error=str(e))
        click.echo("\n  Run `clair validate` to see every routing problem.\n", err=True)
        sys.exit(1)
    except ClairError as e:
        logger.error("run.error", error=str(e))
        sys.exit(1)

    if not summary.results:
        click.echo("Clair found no Trouves to run.")
        return

    failed_tests = [t for t in summary.test_results if not t.passed]
    if summary.failed_count or failed_tests:
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
    select: tuple[str, ...], exclude: tuple[str, ...], env: str | None, sample: bool, threads: int | None
) -> None:
    """Run the data quality tests on Snowflake."""
    try:
        summary = clair_api.test(
            select=select,
            exclude=exclude,
            env=_load_environment(env),
            sample=sample,
            threads=threads,
        )
    except ClairError as e:
        logger.error("test.error", error=str(e))
        sys.exit(1)

    if any(not test_result.passed for test_result in summary.results):
        sys.exit(1)


@cli.command()
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
def clean(before: str | None, dry_run: bool, yes: bool) -> None:
    """Delete the compiled artifacts in _clairtifacts/."""
    try:
        # A dry run first. The user confirms, and clair then removes the runs.
        plan = clair_api.clean(before=before, dry_run=True)
    except InvalidBeforeSpecError as error:
        raise click.BadParameter(str(error), param_hint="--before") from None
    except ClairError as error:
        logger.error("clean.error", error=str(error))
        sys.exit(1)

    if not plan.artifacts_dir_exists:
        click.echo(
            f"Clair found no {ARTIFACTS_DIR_NAME}/ directory. There is nothing to remove."
        )
        return

    if not plan.runs:
        click.echo("There is nothing to remove.")
        return

    verb = "Clair will remove" if dry_run else "Clair removes"
    click.echo(f"{verb} {plan.run_count} artifact run(s):")
    for run in plan.runs:
        time_text = (
            run.created_at.strftime("%Y-%m-%d %H:%M:%S UTC")
            if run.created_at
            else "unknown time"
        )
        click.echo(f"  {run.run_id}  ({time_text})")

    if dry_run:
        return

    if not yes:
        click.confirm(f"\nRemove {plan.run_count} run(s)?", abort=True)

    clair_api.clean(before=before)
    click.echo(f"Clair removed {plan.run_count} run(s).")


if __name__ == "__main__":
    cli()
