"""Make the empty schema of one run.

The `Prepare the schema` step of `.github/workflows/integration.yml` runs this
module one time, before pytest starts. The run starts with a drop, because a
second commit of one pull request reuses the schema name of the first, and a
Trouve that the commit deleted would stay.

The drop belongs here and not in a fixture. A parallel run starts one worker
process for each group of tests, and each worker runs the session fixtures. A
drop in a fixture would remove the tables of a worker that still runs.

`load_source_tables` stays in this module, and the `example_sources` fixture
calls it. The name of each copy holds the prefix of one test class, thus the
clone happens for each class, and not one time for the run.

Run it as a module to make a schema by hand:

    uv run python -m tests.integration.setup --schema-name PR_42
"""

from __future__ import annotations

import argparse

from clair.adapters.snowflake import SnowflakeAdapter
from tests.integration.clean_up import drop_schema
from tests.integration.config import (
    DATABASE_NAME,
    IntegrationConfig,
    load_config,
    normalise_schema_name,
)
from tests.integration.projects import (
    example_project_paths,
    golden_table_name,
    physical_table_name,
    source_logical_names,
    trouves_of,
)
from tests.integration.warehouse import connect, execute


def create_schema(adapter: SnowflakeAdapter, schema_name: str) -> None:
    """Make the schema of the run. An existing schema keeps its content."""
    execute(adapter, f"create schema if not exists {DATABASE_NAME}.{schema_name}")


def load_source_tables(adapter: SnowflakeAdapter, schema_name: str) -> list[str]:
    """Clone the golden source table of each example project into the schema.

    A clone is a zero copy operation. The run can write to its copy, thus the
    golden table never changes.

    Returns:
        The name of each table that this function made.
    """
    made: list[str] = []
    for project_path in example_project_paths():
        for logical_name in source_logical_names(trouves_of(project_path)):
            target = f"{DATABASE_NAME}.{schema_name}.{physical_table_name(logical_name)}"
            golden = f"{DATABASE_NAME}.{golden_table_name(project_path, logical_name)}"
            execute(adapter, f"create or replace table {target} clone {golden}")
            made.append(target)
    return made


def prepare_schema(config: IntegrationConfig) -> None:
    """Drop the schema of the run, and make it again, empty."""
    adapter = connect(config)
    try:
        drop_schema(adapter, config.schema_name)
        create_schema(adapter, config.schema_name)
    finally:
        adapter.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--schema-name", help="The schema of the run. The default comes from the environment."
    )
    arguments = parser.parse_args()

    config = load_config()
    if arguments.schema_name:
        config = IntegrationConfig(
            **{
                **config.__dict__,
                "schema_name": normalise_schema_name(arguments.schema_name),
            }
        )

    prepare_schema(config)
    print(f"{DATABASE_NAME}.{config.schema_name}: the schema is empty")


if __name__ == "__main__":
    main()
