"""Make the schema of one run, and put the source tables in it.

Run it as a module to prepare a schema by hand:

    uv run python -m tests.integration.setup --schema-name PR_42
"""

from __future__ import annotations

import argparse
from pathlib import Path

from clair.adapters.snowflake import SnowflakeAdapter
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

SEEDS_DIR = Path(__file__).parent / "seeds"

# example_3 needs a young timestamp in each row, thus it has no golden table.
SEED_FILES = {"example_3_database.source.orders": "example_3_orders.sql"}


def create_schema(adapter: SnowflakeAdapter, schema_name: str) -> None:
    """Make the schema of the run. An existing schema keeps its content."""
    execute(adapter, f"create schema if not exists {DATABASE_NAME}.{schema_name}")


def load_source_tables(adapter: SnowflakeAdapter, schema_name: str) -> list[str]:
    """Put the source table of each example project in the schema of the run.

    A table with a golden copy arrives as a zero copy clone. example_3 has no
    golden copy, thus its statement makes the rows again.

    Returns:
        The physical name of each table that this function made.
    """
    made: list[str] = []
    for project_path in example_project_paths():
        for logical_name in source_logical_names(trouves_of(project_path)):
            target = f"{DATABASE_NAME}.{schema_name}.{physical_table_name(logical_name)}"
            seed_file = SEED_FILES.get(logical_name)
            if seed_file:
                statement = (SEEDS_DIR / seed_file).read_text()
                execute(adapter, statement.format(physical_name=target))
            else:
                golden = f"{DATABASE_NAME}.{golden_table_name(project_path, logical_name)}"
                execute(adapter, f"create or replace table {target} clone {golden}")
            made.append(target)
    return made


def prepare(config: IntegrationConfig) -> list[str]:
    """Make the schema of the run and load the source tables."""
    adapter = connect(config)
    try:
        create_schema(adapter, config.schema_name)
        return load_source_tables(adapter, config.schema_name)
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

    tables = prepare(config)
    print(f"{DATABASE_NAME}.{config.schema_name}: {len(tables)} source tables")


if __name__ == "__main__":
    main()
