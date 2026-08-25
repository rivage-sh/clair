"""Make a small project that shows what staging does.

The example projects pass their data quality tests, which is correct for a
fixture. Staging is about the run that **fails**, so these tests need a project
whose test fails on demand.

The project holds three Trouves:

    <database_name>.source.rows        A SOURCE. The test makes this table.
    <database_name>.refined.checked    The candidate, with one TestRowCount.
    <database_name>.derived.downstream A TABLE that reads refined.checked.

``minimum_rows`` decides the result. A low number passes, and a number above the
row count of the source fails. The downstream Trouve shows that clair skips each
dependent of a Trouve that failed.

``execution_type`` selects the engine of the candidate. The two engines write in
different ways, and staging must hold for both:

* SNOWFLAKE runs ``CREATE OR REPLACE TABLE`` at the staging address.
* PANDAS calls ``write_pandas``, and that function makes the staging table
  itself. Clair then promotes that table with a clone.

Each test gives its own ``database_name``, thus two tests never write one table
in the schema of the run.

Each function below gives the text of one file. The file that the function
writes holds an f-string of its own, and ``{{source_rows}}`` is that inner
f-string. The outer f-string gives one pair of braces to the file.
"""

from __future__ import annotations

from pathlib import Path

from clair.trouves.trouve import ExecutionType
from tests.integration.projects import CI_DATABASE_CONFIG_FILE, CI_ROUTING_FILE


def source_file() -> str:
    """Give the SOURCE Trouve. The test makes the table that it names."""
    return '''\
from clair import Column, ColumnType, Trouve, TrouveType

trouve = Trouve(
    type=TrouveType.SOURCE,
    docs="The rows that the test inserts before it runs clair.",
    columns=[
        Column(name="id", type=ColumnType.STRING),
    ],
)
'''


def sql_checked_file(database_name: str, minimum_rows: int) -> str:
    """Give the candidate Trouve that Snowflake materializes from SQL."""
    return f'''\
from {database_name}.source.rows import trouve as source_rows

from clair import Column, ColumnType, TestRowCount, Trouve, TrouveType

trouve = Trouve(
    type=TrouveType.TABLE,
    docs="The candidate. TestRowCount decides if clair promotes it.",
    sql=f"""
        select id from {{source_rows}}
    """,
    columns=[
        Column(name="id", type=ColumnType.STRING),
    ],
    tests=[TestRowCount(min_rows={minimum_rows})],
)
'''


def pandas_checked_file(database_name: str, minimum_rows: int) -> str:
    """Give the candidate Trouve that a transform function gives."""
    return f'''\
import pandas as pd
from {database_name}.source.rows import trouve as source_rows

from clair import Column, ColumnType, PandasTrouve, TestRowCount


def keep_each_row(rows: pd.DataFrame) -> pd.DataFrame:
    return rows


trouve = PandasTrouve(
    transform=keep_each_row,
    inputs=[source_rows],
    docs="The candidate that write_pandas makes. TestRowCount decides it.",
    columns=[
        Column(name="id", type=ColumnType.STRING),
    ],
    tests=[TestRowCount(min_rows={minimum_rows})],
)
'''


def downstream_file(database_name: str) -> str:
    """Give the dependent Trouve. Clair skips it after the candidate fails."""
    return f'''\
from {database_name}.refined.checked import trouve as refined_checked

from clair import Column, ColumnType, Trouve, TrouveType

trouve = Trouve(
    type=TrouveType.TABLE,
    docs="A dependent. Clair skips it when the test of refined.checked fails.",
    sql=f"""
        select id from {{refined_checked}}
    """,
    columns=[
        Column(name="id", type=ColumnType.STRING),
    ],
)
'''


def write_probe_project(
    destination: Path,
    database_name: str,
    minimum_rows: int,
    execution_type: ExecutionType = ExecutionType.SNOWFLAKE,
) -> Path:
    """Write the project, and give the path of its root.

    Args:
        destination: The directory that receives the project.
        database_name: The first part of each logical address. Give a different
            name to each test, thus the tables of the tests stay apart.
        minimum_rows: The limit of TestRowCount on the refined Trouve.
        execution_type: SNOWFLAKE for a SQL candidate, PANDAS for a candidate
            that a transform function gives.

    Returns:
        The path of the project root.

    Raises:
        ValueError: If the execution type has no Trouve here.
    """
    project_path = destination / database_name
    database_path = project_path / database_name
    for schema_name in ("source", "refined", "derived"):
        (database_path / schema_name).mkdir(parents=True, exist_ok=True)

    (project_path / "__routing__.py").write_text(CI_ROUTING_FILE)
    (database_path / "__database_config__.py").write_text(CI_DATABASE_CONFIG_FILE)

    checked_file = None
    if execution_type == ExecutionType.PANDAS:
        checked_file = pandas_checked_file(database_name, minimum_rows)
    elif execution_type == ExecutionType.SNOWFLAKE:
        checked_file = sql_checked_file(database_name, minimum_rows)
    else:
        raise ValueError(f"The probe project has no Trouve for {execution_type}.")

    (database_path / "source" / "rows.py").write_text(source_file())
    (database_path / "refined" / "checked.py").write_text(checked_file)
    (database_path / "derived" / "downstream.py").write_text(
        downstream_file(database_name)
    )
    return project_path
