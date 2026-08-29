"""Compare the data of two tables in Snowflake.

A staging test proves that clair *said* what it did: the `RunSummary` gives the
effective run mode, the statements, and each test result. It does not prove that
the data is correct. `tables_are_equal` closes that hole. A test builds the same
data a second time along a different path, and then it asks this module if the
two tables hold the same rows.

The first use is the fallback of an incremental run. Clair finds no physical
table on the first run, thus it changes to a full refresh. A test that reads
`effective_run_mode` learns the intent of clair. A test that runs a real full
refresh into a second table, and then calls `tables_are_equal`, learns the
result.

How the comparison works
------------------------

1. Read the name and the data type of each column of the two tables, from
   ``INFORMATION_SCHEMA.COLUMNS``. The two tables must hold the same set of
   column names, or the comparison stops.
2. Join the two tables on the primary key columns, with ``EQUAL_NULL``. A NULL
   in a key thus joins to a NULL, which ``=`` does not do. The join is a FULL
   OUTER JOIN, therefore a row that one table alone holds also comes back.
3. Make one ``is_equal_<column>`` expression for each column that is not a key:

   * A column of an exact type gives ``EQUAL_NULL(a.column, b.column)``.
   * A column of a float type gives a comparison inside an absolute bound and a
     relative bound. A float that a different statement computes holds a
     different last bit, and an exact comparison then reports a difference that
     no reader cares about.

4. Select each row where any ``is_equal_<column>`` expression is false, or where
   one table holds no row at all.

The result holds those rows. No row means that the two tables are equal.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from clair.adapters.snowflake import SnowflakeAdapter
from clair.trouves.address import TrouveAddress
from tests.integration.warehouse import query_rows

# The data types of Snowflake that hold a binary float. Each one loses the last
# bits, thus these columns get the tolerance comparison. NUMBER is exact, even
# with a scale, so NUMBER takes the exact comparison.
FLOAT_DATA_TYPES = frozenset({"FLOAT", "FLOAT4", "FLOAT8", "DOUBLE", "DOUBLE PRECISION", "REAL"})

# The default bounds of the float comparison. The absolute bound holds for a
# value near zero, where a relative bound gives almost nothing. The relative
# bound holds for a large value. A difference inside one of the two bounds is
# not a difference.
DEFAULT_ABSOLUTE_TOLERANCE = 1e-9
DEFAULT_RELATIVE_TOLERANCE = 1e-9

# The largest number of rows that the comparison reads back. A test that fails
# needs a few rows to name the problem, and it does not need the full table.
DEFAULT_MAX_DIFFERENCE_ROWS = 20

_LEFT = "table_a"
_RIGHT = "table_b"

# The marker column that each side of the join gets. It tells the WHERE clause
# which side holds the row. The name must not collide with a real column.
_MARKER = "_clair_row_marker"


class TableComparisonError(RuntimeError):
    """The two tables cannot be compared."""


@dataclass
class TableComparison:
    """What `tables_are_equal` found.

    Attributes:
        is_equal: True when the two tables hold the same rows.
        difference_rows: Each row that differs, up to the maximum. Each row
            holds the key columns first, then one flag for each compared
            column, then the two values of each column that differs.
        difference_column_names: The name of each column of `difference_rows`.
        compared_column_names: Each column that the comparison read.
        float_column_names: Each column that took the tolerance comparison.
        sql: The query that made the result. A failed test prints it, thus a
            reader repeats the comparison by hand.
    """

    is_equal: bool
    difference_rows: list[tuple[object, ...]] = field(default_factory=list)
    difference_column_names: list[str] = field(default_factory=list)
    compared_column_names: list[str] = field(default_factory=list)
    float_column_names: list[str] = field(default_factory=list)
    sql: str = ""

    def __bool__(self) -> bool:
        return self.is_equal

    def report(self) -> str:
        """Give a message for the `assert` statement of a test."""
        if self.is_equal:
            return "The two tables hold the same rows."

        lines = [
            (
                f"The two tables differ. {len(self.difference_rows)} row(s) "
                f"came back (the limit applies)."
            ),
            f"Columns: {', '.join(self.difference_column_names)}",
        ]
        lines.extend(f"  {row}" for row in self.difference_rows)
        lines.append(f"SQL:\n{self.sql}")
        return "\n".join(lines)


def column_types_of(
    adapter: SnowflakeAdapter, address: TrouveAddress
) -> dict[str, str]:
    """Give the data type of each column of one table, by the column name.

    Snowflake holds each name in upper case in INFORMATION_SCHEMA, thus the keys
    of the result are upper case.
    """
    rows = query_rows(
        adapter,
        "SELECT COLUMN_NAME, DATA_TYPE "
        f"FROM {address.database_name}.INFORMATION_SCHEMA.COLUMNS "
        f"WHERE TABLE_SCHEMA = '{address.schema_name.upper()}' "
        f"AND TABLE_NAME = '{address.table_name.upper()}' "
        "ORDER BY ORDINAL_POSITION",
    )
    types = {str(row[0]).upper(): str(row[1]).upper() for row in rows}
    if not types:
        raise TableComparisonError(
            f"{address} holds no column. The table does not exist, or the role "
            "of the test cannot read it."
        )
    return types


def _is_equal_expression(
    column_name: str,
    data_type: str,
    absolute_tolerance: float,
    relative_tolerance: float,
) -> str:
    """Give the SQL that tells you if one column holds the same value."""
    left = f'{_LEFT}."{column_name}"'
    right = f'{_RIGHT}."{column_name}"'

    if data_type not in FLOAT_DATA_TYPES:
        return f"EQUAL_NULL({left}, {right})"

    # A NULL on one side, or a NULL on each side, takes the exact path: a
    # subtraction of a NULL gives a NULL, and a NULL is not a difference of
    # zero. ABS of a NaN also gives a NaN, thus a NaN reports a difference.
    return (
        "CASE "
        f"WHEN {left} IS NULL OR {right} IS NULL THEN EQUAL_NULL({left}, {right}) "
        f"ELSE ABS({left} - {right}) <= {absolute_tolerance} "
        f"OR ABS({left} - {right}) "
        f"<= {relative_tolerance} * GREATEST(ABS({left}), ABS({right})) "
        "END"
    )


def build_comparison_sql(
    address_a: TrouveAddress,
    address_b: TrouveAddress,
    primary_key_columns: list[str],
    column_types: dict[str, str],
    absolute_tolerance: float = DEFAULT_ABSOLUTE_TOLERANCE,
    relative_tolerance: float = DEFAULT_RELATIVE_TOLERANCE,
    max_difference_rows: int = DEFAULT_MAX_DIFFERENCE_ROWS,
) -> tuple[str, list[str]]:
    """Make the query that gives each row that differs.

    Returns:
        The SQL, and the name of each column of the result.
    """
    keys = [name.upper() for name in primary_key_columns]
    value_columns = [name for name in column_types if name not in keys]
    join_condition = " AND ".join(
        f'EQUAL_NULL({_LEFT}."{key}", {_RIGHT}."{key}")' for key in keys
    )

    # A FULL OUTER JOIN keeps a row that one table alone holds. COALESCE gives
    # the key of that row, whichever side holds it.
    select_parts = [
        f'COALESCE({_LEFT}."{key}", {_RIGHT}."{key}") AS "{key}"' for key in keys
    ]
    result_column_names = list(keys)

    # Each side gets a marker column with the value TRUE. After the FULL OUTER
    # JOIN the marker is NULL on the side that holds no row. A key that is NULL
    # on each side joins correctly with EQUAL_NULL, and the marker then stays
    # TRUE, thus the marker separates "no row" from "a NULL key".
    select_parts.append(f"{_LEFT}.{_MARKER} IS NOT NULL AS is_row_in_a")
    select_parts.append(f"{_RIGHT}.{_MARKER} IS NOT NULL AS is_row_in_b")
    result_column_names.append("is_row_in_a")
    result_column_names.append("is_row_in_b")

    flag_names = []
    for column_name in value_columns:
        flag = f"is_equal_{column_name.lower()}"
        expression = _is_equal_expression(
            column_name,
            column_types[column_name],
            absolute_tolerance,
            relative_tolerance,
        )
        select_parts.append(f"{expression} AS {flag}")
        flag_names.append(flag)
        result_column_names.append(flag)

    # The two values of each column, so a reader sees what differs.
    for column_name in value_columns:
        select_parts.append(f'{_LEFT}."{column_name}" AS "a_{column_name.lower()}"')
        select_parts.append(f'{_RIGHT}."{column_name}" AS "b_{column_name.lower()}"')
        result_column_names.append(f"a_{column_name.lower()}")
        result_column_names.append(f"b_{column_name.lower()}")

    # A row that one table alone holds is a difference, and so is a flag that
    # is false. A flag of a row that lost its partner gives NULL, thus COALESCE
    # gives it the value FALSE and the row still comes back.
    conditions = [f"NOT COALESCE({flag}, FALSE)" for flag in flag_names]
    conditions.append(f"{_LEFT}.{_MARKER} IS NULL OR {_RIGHT}.{_MARKER} IS NULL")
    where_clause = " OR ".join(conditions)

    sql = (
        "SELECT " + ", ".join(select_parts) + "\n"
        f"FROM (SELECT *, TRUE AS {_MARKER} FROM {address_a}) AS {_LEFT}\n"
        f"FULL OUTER JOIN (SELECT *, TRUE AS {_MARKER} FROM {address_b}) "
        f"AS {_RIGHT}\n"
        f"  ON {join_condition}\n"
        f"WHERE {where_clause}\n"
        f"LIMIT {max_difference_rows}"
    )
    return sql, result_column_names


def tables_are_equal(
    adapter: SnowflakeAdapter,
    address_a: TrouveAddress,
    address_b: TrouveAddress,
    primary_key_columns: list[str],
    absolute_tolerance: float = DEFAULT_ABSOLUTE_TOLERANCE,
    relative_tolerance: float = DEFAULT_RELATIVE_TOLERANCE,
    max_difference_rows: int = DEFAULT_MAX_DIFFERENCE_ROWS,
) -> TableComparison:
    """Tell you if two tables in Snowflake hold the same rows.

    Args:
        adapter: An open connection.
        address_a: The first table.
        address_b: The second table.
        primary_key_columns: The column, or the columns, that name one row. The
            comparison joins the two tables on them with EQUAL_NULL, thus a NULL
            key joins to a NULL key.
        absolute_tolerance: The largest absolute difference of a float column
            that is not a difference.
        relative_tolerance: The largest relative difference of a float column
            that is not a difference.
        max_difference_rows: The largest number of rows that come back.

    Returns:
        A TableComparison. `is_equal` is True when the two tables match, and
        `difference_rows` then holds nothing.

    Raises:
        TableComparisonError: If a table holds no column, if the two tables hold
            a different set of columns, or if a key column is absent.
    """
    if not primary_key_columns:
        raise TableComparisonError("The comparison needs one primary key column.")

    types_a = column_types_of(adapter, address_a)
    types_b = column_types_of(adapter, address_b)

    if set(types_a) != set(types_b):
        only_a = sorted(set(types_a) - set(types_b))
        only_b = sorted(set(types_b) - set(types_a))
        raise TableComparisonError(
            f"{address_a} and {address_b} hold a different set of columns. "
            f"Only in the first: {only_a}. Only in the second: {only_b}."
        )

    keys = [name.upper() for name in primary_key_columns]
    absent_keys = [key for key in keys if key not in types_a]
    if absent_keys:
        raise TableComparisonError(
            f"{address_a} holds no column with the name {absent_keys}. "
            f"The table holds these columns: {sorted(types_a)}."
        )

    sql, result_column_names = build_comparison_sql(
        address_a,
        address_b,
        keys,
        types_a,
        absolute_tolerance,
        relative_tolerance,
        max_difference_rows,
    )
    rows = query_rows(adapter, sql)

    return TableComparison(
        is_equal=len(rows) == 0,
        difference_rows=rows,
        difference_column_names=result_column_names,
        compared_column_names=sorted(types_a),
        float_column_names=sorted(
            name for name, type_name in types_a.items()
            if type_name in FLOAT_DATA_TYPES and name not in keys
        ),
        sql=sql,
    )
