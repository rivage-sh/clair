"""The time of discovery and compile grows with the number of files, and no faster.

A large project holds thousands of Trouve files. A step that scans every module
of the process for each file makes the time grow with files times modules, and
such a project then waits minutes for each command. The test writes a project
at test time, at two sizes, and compares the two times. A limit on the time
itself fails on a slow machine, thus the test states the growth only.

A scan also reads each library module of the process, approximately two
thousand, and that part grows with the files alone. It hides the quadratic part
at a small factor of size, thus the test compares two sizes with a factor of
four.

The Trouve files hold f-strings of their own, thus the generated text writes
two braces, for example ``{{upstream}}``.
"""

from __future__ import annotations

import time
from pathlib import Path

import pytest

import clair
from tests.helpers import write_project_marker

DATABASE_COUNT = 4
SCHEMAS_PER_DATABASE = 5

SMALL_TROUVE_COUNT = 500
LARGE_TROUVE_COUNT = 4 * SMALL_TROUVE_COUNT

# Linear growth gives approximately 4. A scan of sys.modules for each file gave
# more than 6. The limit sits between the two, with room for the noise of a
# shared CI machine.
MAXIMUM_GROWTH = 5.0

SHARED_SQL_MODULE = """\
def deduplicated(upstream) -> str:
    return f"select distinct * from {upstream}"
"""


def source_file() -> str:
    return """\
from clair import Column, ColumnType, Source

trouve = Source(
    columns=[Column(name="id", type=ColumnType.INTEGER)],
)
"""


def table_file(upstream_module: str, source_module: str) -> str:
    return f"""\
from {upstream_module} import trouve as upstream
from {source_module} import trouve as source
from _shared_sql import deduplicated

from clair import THIS, Column, ColumnType, Table, TestSql, TestUnique

trouve = Table(
    sql=f"select u.id from ({{deduplicated(upstream)}}) u join {{source}} s on u.id = s.id",
    columns=[Column(name="id", type=ColumnType.INTEGER)],
    tests=[
        TestUnique(column="id"),
        TestSql(sql=f"select * from {{THIS}} t left join {{source}} s on t.id = s.id where s.id is null"),
    ],
)
"""


def config_file(class_name: str, warehouse_name: str) -> str:
    return f"""\
from clair import {class_name}

defaults = {class_name}(warehouse="{warehouse_name}")
"""


def write_large_project(project_root: Path, trouve_count: int) -> Path:
    """Write a project of *trouve_count* Trouves, spread over several schemas.

    Each schema holds one SOURCE and a chain of TABLEs. Each TABLE reads the
    TABLE before it and the SOURCE, through a shared SQL function, and holds
    two tests. Each database and each schema holds a configuration file.
    """
    write_project_marker(project_root)
    (project_root / "_shared_sql.py").write_text(SHARED_SQL_MODULE)
    schema_count = DATABASE_COUNT * SCHEMAS_PER_DATABASE
    trouves_per_schema = max(2, trouve_count // schema_count)

    for database_index in range(DATABASE_COUNT):
        database_name = f"db{database_index}"
        database_directory = project_root / database_name
        database_directory.mkdir(parents=True)
        (database_directory / "__database_config__.py").write_text(
            config_file("DatabaseDefaults", f"{database_name}_wh")
        )
        for schema_index in range(SCHEMAS_PER_DATABASE):
            schema_name = f"schema{schema_index}"
            schema_directory = database_directory / schema_name
            schema_directory.mkdir()
            (schema_directory / "__schema_config__.py").write_text(
                config_file("SchemaDefaults", f"{schema_name}_wh")
            )
            source_module = f"{database_name}.{schema_name}.table0"
            (schema_directory / "table0.py").write_text(source_file())
            for table_index in range(1, trouves_per_schema):
                upstream_module = f"{database_name}.{schema_name}.table{table_index - 1}"
                (schema_directory / f"table{table_index}.py").write_text(
                    table_file(upstream_module, source_module)
                )
    return project_root


def seconds_to_compile(project_root: Path) -> float:
    """Give the best time of two compiles. The best time holds the least noise."""
    durations: list[float] = []
    for _ in range(2):
        start = time.perf_counter()
        clair.compile(project_root)
        durations.append(time.perf_counter() - start)
    return min(durations)


@pytest.mark.parametrize("trouve_count", [40, 400])
def test_the_generated_project_compiles(tmp_path: Path, trouve_count: int) -> None:
    """The generated project is valid, thus the time test measures real work."""
    project_root = write_large_project(tmp_path / "project", trouve_count)

    report = clair.validate(project_root)
    output = clair.compile(project_root)

    schema_count = DATABASE_COUNT * SCHEMAS_PER_DATABASE
    assert report.is_valid
    assert output.source_count == schema_count
    assert output.trouve_count + output.source_count == trouve_count
    for node in output.compiled_nodes:
        database_name, schema_name, table_name = str(node.addresses.logical).split(".")
        table_index = int(table_name.removeprefix("table"))
        expected = {
            f"{database_name}.{schema_name}.table0",
            f"{database_name}.{schema_name}.table{table_index - 1}",
        }
        assert set(node.dependencies) == expected


def test_compile_time_grows_linearly_with_the_project(tmp_path: Path) -> None:
    small_project = write_large_project(tmp_path / "small", SMALL_TROUVE_COUNT)
    large_project = write_large_project(tmp_path / "large", LARGE_TROUVE_COUNT)

    small_seconds = seconds_to_compile(small_project)
    large_seconds = seconds_to_compile(large_project)

    assert large_seconds < MAXIMUM_GROWTH * small_seconds
