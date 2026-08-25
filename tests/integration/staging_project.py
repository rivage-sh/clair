"""Make a small project that shows what staging does.

The example projects pass their data quality tests, which is correct for a
fixture. Staging is about the run that **fails**, so these tests need a project
whose test fails on demand.

The project holds three Trouves:

    <database_name>.source.rows        A SOURCE. The test makes this table.
    <database_name>.refined.checked    A TABLE with one TestRowCount.
    <database_name>.derived.downstream A TABLE that reads refined.checked.

``minimum_rows`` decides the result. A low number passes, and a number above the
row count of the source fails. The downstream Trouve shows that clair skips each
dependent of a Trouve that failed.

Each test gives its own ``database_name``, thus two tests never write one table
in the schema of the run.
"""

from __future__ import annotations

from pathlib import Path

from tests.integration.projects import CI_DATABASE_CONFIG_FILE, CI_ROUTING_FILE

_SOURCE_FILE = '''\
from clair import Column, ColumnType, Trouve, TrouveType

trouve = Trouve(
    type=TrouveType.SOURCE,
    docs="The rows that the test inserts before it runs clair.",
    columns=[
        Column(name="id", type=ColumnType.STRING),
    ],
)
'''

_CHECKED_FILE = '''\
from __DATABASE_NAME__.source.rows import trouve as source_rows

from clair import Column, ColumnType, TestRowCount, Trouve, TrouveType

trouve = Trouve(
    type=TrouveType.TABLE,
    docs="The candidate. TestRowCount decides if clair promotes it.",
    sql=f"""
        select id from {source_rows}
    """,
    columns=[
        Column(name="id", type=ColumnType.STRING),
    ],
    tests=[TestRowCount(min_rows=__MINIMUM_ROWS__)],
)
'''

_DOWNSTREAM_FILE = '''\
from __DATABASE_NAME__.refined.checked import trouve as refined_checked

from clair import Column, ColumnType, Trouve, TrouveType

trouve = Trouve(
    type=TrouveType.TABLE,
    docs="A dependent. Clair skips it when the test of refined.checked fails.",
    sql=f"""
        select id from {refined_checked}
    """,
    columns=[
        Column(name="id", type=ColumnType.STRING),
    ],
)
'''


def write_probe_project(
    destination: Path, database_name: str, minimum_rows: int
) -> Path:
    """Write the project, and give the path of its root.

    Args:
        destination: The directory that receives the project.
        database_name: The first part of each logical address. Give a different
            name to each test, thus the tables of the tests stay apart.
        minimum_rows: The limit of TestRowCount on the refined Trouve.

    Returns:
        The path of the project root.
    """
    project_path = destination / database_name
    database_path = project_path / database_name
    for schema_name in ("source", "refined", "derived"):
        (database_path / schema_name).mkdir(parents=True, exist_ok=True)

    (project_path / "__routing__.py").write_text(CI_ROUTING_FILE)
    (database_path / "__database_config__.py").write_text(CI_DATABASE_CONFIG_FILE)

    (database_path / "source" / "rows.py").write_text(_SOURCE_FILE)
    (database_path / "refined" / "checked.py").write_text(
        _CHECKED_FILE.replace("__DATABASE_NAME__", database_name).replace(
            "__MINIMUM_ROWS__", str(minimum_rows)
        )
    )
    (database_path / "derived" / "downstream.py").write_text(
        _DOWNSTREAM_FILE.replace("__DATABASE_NAME__", database_name)
    )
    return project_path
