"""Drop the schema of one run.

    uv run python -m tests.integration.clean_up --schema-name PR_42

The workflow that runs when a pull request closes calls this module.
"""

from __future__ import annotations

import argparse

from clair.adapters.snowflake import SnowflakeAdapter
from tests.integration.config import (
    DATABASE_NAME,
    PROTECTED_SCHEMA_NAMES,
    load_config,
    normalise_schema_name,
)
from tests.integration.warehouse import connect, execute


def drop_schema(adapter: SnowflakeAdapter, schema_name: str) -> None:
    """Drop one schema of a run.

    ``normalise_schema_name`` refuses a golden schema and an unusual character,
    thus the name that reaches the DROP statement is always safe.
    """
    safe_name = normalise_schema_name(schema_name)
    if safe_name in PROTECTED_SCHEMA_NAMES:
        raise ValueError(f"{safe_name} is protected.")
    execute(adapter, f"drop schema if exists {DATABASE_NAME}.{safe_name} cascade")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--schema-name",
        help="The schema to drop. The default is CLAIR_PR_TESTING_SCHEMA_NAME.",
    )
    arguments = parser.parse_args()

    config = load_config(arguments.schema_name)
    schema_name = config.schema_name

    adapter = connect(config)
    try:
        drop_schema(adapter, schema_name)
    finally:
        adapter.close()
    print(f"dropped {DATABASE_NAME}.{normalise_schema_name(schema_name)}")


if __name__ == "__main__":
    main()
