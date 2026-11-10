"""The Clair CLI. This module is the click entry point."""

from __future__ import annotations

import sys
from pathlib import Path

import click
import structlog

from clair import api as clair_api
from clair._logging import configure_logging
from clair._version import __version__
from clair.core.artifacts import (
    InvalidBeforeSpecError,
    write_compile_output,
    write_run_result,
    write_run_summary,
)
from clair.core.dag import build_dag
from clair.core.dag_render import render_dag
from clair.core.discovery import ARTIFACTS_DIR_NAME, discover_project
from clair.core.scaffold import scaffold_project, write_environments_yml
from clair.environments.environments import (
    DEFAULT_THREADS,
    Authenticator,
    Connection,
    Environment,
    Password,
    PrivateKey,
    load_environment,
)
from clair.environments.project_routing import ROUTING_FILE_NAME, find_project_root
from clair.exceptions import (
    ClairError,
    EnvironmentNotFoundError,
    EnvironmentsFileNotFoundError,
    InvalidRoutingConfigError,
    InvalidTrouveAddressError,
    ProjectRootNotFoundError,
)
from clair.trouves.run_config import RunMode
from clair.trouves.trouve import DEFAULT_SAMPLE_ROWS

logger = structlog.get_logger()

# `clair compile`, `clair run` and `clair test` take the same sample options.
_env_option = click.option(
    "--env",
    required=True,
    envvar="CLAIR_ENV",
    help="The environment name. It selects the entry of __routing__.py and the profile of ~/.clair/environments.yml. CLAIR_ENV gives it when you omit --env.",
)

_sample_rows_option = click.option(
    "--sample-rows",
    type=click.IntRange(min=1),
    default=None,
    help=f"The rows of each sample, for a Trouve without a sample_sql. The default is {DEFAULT_SAMPLE_ROWS}. It needs --sample.",
)


def _sample_rows(sample: bool, sample_rows: int | None) -> int:
    """Give the row limit of the sample, and stop a --sample-rows without --sample."""
    if sample_rows is None:
        return DEFAULT_SAMPLE_ROWS
    if not sample:
        raise click.UsageError("--sample-rows needs --sample.")
    return sample_rows


def _artifacts_dir_of(project_root: Path) -> Path:
    """Give the directory that holds the artifacts of each compile and each run.

    The API writes no file. The CLI is the caller that decides where the bytes go.
    """
    return project_root / ARTIFACTS_DIR_NAME


def _load_environment(env_name: str) -> Environment:
    """Read ~/.clair/environments.yml, and give the environment of *env_name*.

    The CLI reads the file. The Python API functions accept the Environment
    object, thus a caller that holds the settings writes no file.
    """
    return load_environment(env_name)


def _load_environment_or_name(env_name: str) -> Environment | str:
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
        return env_name


@click.group()
@click.version_option(version=__version__, prog_name="clair")
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

    # Step 2 -- The environment. The routing file and the next steps name it.
    environments_path = Path.home() / ".clair" / "environments.yml"
    if environments_path.exists():
        click.echo("  ~/.clair/environments.yml exists. Clair keeps it.")
        env_name = _prompt_for_existing_environment(environments_path)
    else:
        env_name = _prompt_and_write_environment()

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
        env_name=env_name,
    )

    click.echo("")
    for status, filepath in results:
        # Hide the environments.yml line. The code above found that file, or
        # the user gave the values for it.
        if filepath == str(environments_path):
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
    click.echo(f"  2. clair compile --env {env_name}")
    click.echo(f"  3. clair run --env {env_name}")
    click.echo("")


def _prompt_for_existing_environment(environments_path: Path) -> str:
    """Ask for an environment of the existing environments.yml, and give its name."""
    click.echo("")
    while True:
        env_name = click.prompt("Environment name", default="dev", type=str)
        try:
            load_environment(env_name, environments_path=environments_path)
        except EnvironmentNotFoundError as exc:
            click.echo(str(exc), err=True)
            continue
        except ClairError as exc:
            click.echo(f"Error: {exc}", err=True)
            sys.exit(1)
        return env_name


def _prompt_and_write_environment() -> str:
    """Ask the user for the Snowflake connection data and write environments.yml.

    Returns:
        The name of the new environment.
    """

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
    auth_choice = click.prompt("Enter choice", default="1", type=click.Choice(["1", "2", "3"]))

    authentication = None
    if auth_choice == "1":
        private_key_path = _require("Private key path")
        passphrase = None
        if click.confirm("Is the key encrypted?", default=False):
            passphrase = click.prompt("Private key passphrase", hide_input=True, type=str)
        authentication = PrivateKey(
            private_key_path=private_key_path, private_key_passphrase=passphrase
        )
    elif auth_choice == "2":
        authentication = Password(
            password=click.prompt("Password", hide_input=True, type=str)
        )
    elif auth_choice == "3":
        authentication = Authenticator(authenticator="externalbrowser")
    else:
        raise click.UsageError(f"No authentication method for the choice {auth_choice}.")

    click.echo("")
    _hint("current_warehouse() as warehouse")
    warehouse = _require("Warehouse")

    click.echo("")
    role = click.prompt("Role (leave empty to use the default role of the user)", default="", type=str, show_default=False)

    click.echo("")
    _hint("current_region() as region")
    region = _require("Region (e.g. us-east-1)")

    click.echo("")
    _hint("current_account() as account_locator")
    account_locator = _require("Account locator (e.g. abc12345)")

    click.echo("")
    threads = click.prompt(
        "Trouves that run at one time (threads)",
        default=DEFAULT_THREADS,
        type=click.IntRange(min=1),
    )

    environment = Environment(
        name=env_name,
        connection=Connection(
            account=account,
            user=user,
            warehouse=warehouse,
            role=role or None,
            region=region,
            account_locator=account_locator,
        ),
        authentication=authentication,
        threads=threads,
    )
    click.echo("")
    write_environments_yml(environment)
    return env_name


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
@_env_option
@click.option(
    "--run-mode",
    type=click.Choice(["full_refresh", "incremental"], case_sensitive=False),
    default="full_refresh",
    help="Run mode. full_refresh writes all tables again. incremental writes only the new data.",
)
@click.option(
    "--sample",
    is_flag=True,
    default=False,
    help="Show the SQL of `clair run --sample`: each Trouve that the run does not build reads its sample.",
)
@_sample_rows_option
def compile_cmd(select: tuple[str, ...], exclude: tuple[str, ...], env: str, run_mode: str, sample: bool, sample_rows: int | None) -> None:
    """Compile the project and show the new SQL. This needs no Snowflake connection."""
    row_limit = _sample_rows(sample, sample_rows)
    try:
        project_root = find_project_root(Path.cwd())
        output = clair_api.compile(
            project_root,
            select=select,
            exclude=exclude,
            env=_load_environment_or_name(env),
            run_mode=RunMode(run_mode),
            sample=sample,
            sample_rows=row_limit,
        )
    except (InvalidRoutingConfigError, InvalidTrouveAddressError) as e:
        logger.error("compile.routing_error", error=str(e))
        click.echo("\n  Run `clair validate` to see every routing problem.\n", err=True)
        sys.exit(1)
    except ClairError as e:
        logger.error("compile.error", error=str(e))
        sys.exit(1)

    run_dir = write_compile_output(_artifacts_dir_of(project_root), output)
    logger.info("compile.artifacts", artifacts_dir=str(run_dir))


@cli.command()
@_env_option
def validate(env: str) -> None:
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
@_env_option
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
    help=(
        "Read a sample of each Trouve that the run does not build: its sample_sql, or a row limit. "
        "Each Trouve must write to an address that routing changed."
    ),
)
@_sample_rows_option
@click.option(
    "--threads",
    type=click.IntRange(min=1),
    default=None,
    help="The number of Trouves that run at one time. The default comes from the environment.",
)
def run(select: tuple[str, ...], exclude: tuple[str, ...], env: str, run_mode: str, no_test: bool, sample: bool, sample_rows: int | None, threads: int | None) -> None:
    """Run the Trouves on Snowflake. Then run the data quality tests."""
    row_limit = _sample_rows(sample, sample_rows)
    try:
        project_root = find_project_root(Path.cwd())
        artifacts_dir = _artifacts_dir_of(project_root)
        with clair_api.start_run(
            project_root,
            select=select,
            exclude=exclude,
            env=_load_environment(env),
            run_mode=RunMode(run_mode),
            test=not no_test,
            sample=sample,
            sample_rows=row_limit,
            threads=threads,
        ) as run_handle:
            # The file of each Trouve is on disk when the Trouve completes, thus
            # a run that stops keeps the files of each Trouve before it.
            for result in run_handle:
                write_run_result(artifacts_dir, run_handle.run_id, result)
        summary = run_handle.summary
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

    summary_file = write_run_summary(artifacts_dir, summary)
    logger.info("run.artifacts", artifacts_dir=str(summary_file.parent))

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
@_env_option
@click.option(
    "--sample",
    is_flag=True,
    default=False,
    help="Run each test on a sample of its Trouve: the sample_sql, or a row limit. Clair does not run the row count tests.",
)
@_sample_rows_option
@click.option(
    "--threads",
    type=click.IntRange(min=1),
    default=None,
    help="The number of Trouves that clair tests at one time. The default comes from the environment.",
)
def test(
    select: tuple[str, ...],
    exclude: tuple[str, ...],
    env: str,
    sample: bool,
    sample_rows: int | None,
    threads: int | None,
) -> None:
    """Run the data quality tests on Snowflake."""
    row_limit = _sample_rows(sample, sample_rows)
    try:
        summary = clair_api.test(
            select=select,
            exclude=exclude,
            env=_load_environment(env),
            sample=sample,
            sample_rows=row_limit,
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
