"""The tests of the SQL that `tests.integration.comparison` builds.

`tables_are_equal` needs a Snowflake connection, thus it runs in the integration
job alone. `build_comparison_sql` needs no connection: it takes the column types
and it gives text. These tests therefore run in each job, and they hold the
rules of the comparison:

* The join uses EQUAL_NULL on each key column.
* A column of an exact type gives EQUAL_NULL.
* A column of a float type gives the two tolerance bounds.
* A row that one table alone holds comes back.
"""

from __future__ import annotations

import pytest

from clair.trouves.address import TrouveAddress
from tests.integration.comparison import (
    DEFAULT_MAX_DIFFERENCE_ROWS,
    build_comparison_sql,
)

ADDRESS_A = TrouveAddress(
    database_name="db", schema_name="schema_name", table_name="incremental_table"
)
ADDRESS_B = TrouveAddress(
    database_name="db", schema_name="schema_name", table_name="full_refresh_table"
)

# One key, one exact column, one float column.
COLUMN_TYPES = {"ID": "TEXT", "AMOUNT": "NUMBER", "RATIO": "FLOAT"}


def sql_of(column_types=None, keys=None, **kwargs) -> str:
    sql, _ = build_comparison_sql(
        ADDRESS_A,
        ADDRESS_B,
        keys or ["ID"],
        column_types or COLUMN_TYPES,
        **kwargs,
    )
    return sql


class TestTheJoin:
    """The key columns join with EQUAL_NULL, and the join keeps each row."""

    def test_the_join_reads_each_key_with_equal_null(self):
        assert 'EQUAL_NULL(table_a."ID", table_b."ID")' in sql_of()

    def test_two_keys_give_two_conditions(self):
        sql = sql_of(
            column_types={"ID": "TEXT", "DAY": "DATE", "AMOUNT": "NUMBER"},
            keys=["ID", "DAY"],
        )

        assert 'EQUAL_NULL(table_a."ID", table_b."ID")' in sql
        assert 'EQUAL_NULL(table_a."DAY", table_b."DAY")' in sql
        assert " AND " in sql

    def test_the_join_is_a_full_outer_join(self):
        """An INNER JOIN hides a row that one table alone holds."""
        assert "FULL OUTER JOIN" in sql_of()

    def test_each_side_holds_a_row_marker(self):
        """The marker separates "no row" from "a key that is NULL"."""
        sql = sql_of()

        assert sql.count("TRUE AS _clair_row_marker") == 2
        assert "table_a._clair_row_marker IS NULL" in sql
        assert "table_b._clair_row_marker IS NULL" in sql

    def test_a_key_is_not_a_compared_column(self):
        """The join holds the key, thus no is_equal_id column exists."""
        assert "is_equal_id" not in sql_of()


class TestTheExactColumns:
    """A column that is not a float compares with EQUAL_NULL."""

    def test_an_exact_column_gives_equal_null(self):
        sql = sql_of()

        assert 'EQUAL_NULL(table_a."AMOUNT", table_b."AMOUNT") AS is_equal_amount' in sql

    def test_an_exact_column_holds_no_tolerance(self):
        sql = sql_of(column_types={"ID": "TEXT", "AMOUNT": "NUMBER"})

        assert "ABS(" not in sql

    @pytest.mark.parametrize("data_type", ["TEXT", "NUMBER", "DATE", "BOOLEAN", "TIMESTAMP_NTZ"])
    def test_each_exact_type_takes_the_exact_path(self, data_type: str):
        sql = sql_of(column_types={"ID": "TEXT", "VALUE": data_type})

        assert 'EQUAL_NULL(table_a."VALUE", table_b."VALUE") AS is_equal_value' in sql


class TestTheFloatColumns:
    """A float column compares inside an absolute bound and a relative bound."""

    def test_a_float_column_reads_the_absolute_bound(self):
        sql = sql_of(absolute_tolerance=1e-6)

        assert 'ABS(table_a."RATIO" - table_b."RATIO") <= 1e-06' in sql

    def test_a_float_column_reads_the_relative_bound(self):
        sql = sql_of(relative_tolerance=1e-3)

        assert 'GREATEST(ABS(table_a."RATIO"), ABS(table_b."RATIO"))' in sql
        assert "0.001 *" in sql

    def test_a_null_float_takes_the_exact_path(self):
        """A subtraction of a NULL gives a NULL, and that is no difference."""
        sql = sql_of()

        assert 'WHEN table_a."RATIO" IS NULL OR table_b."RATIO" IS NULL' in sql
        assert 'THEN EQUAL_NULL(table_a."RATIO", table_b."RATIO")' in sql

    @pytest.mark.parametrize("data_type", ["FLOAT", "DOUBLE", "REAL", "FLOAT8"])
    def test_each_float_type_takes_the_tolerance_path(self, data_type: str):
        sql = sql_of(column_types={"ID": "TEXT", "VALUE": data_type})

        assert 'ABS(table_a."VALUE" - table_b."VALUE")' in sql

    def test_a_number_with_a_scale_stays_exact(self):
        """NUMBER holds an exact decimal, thus it needs no bound."""
        sql = sql_of(column_types={"ID": "TEXT", "PRICE": "NUMBER"})

        assert 'EQUAL_NULL(table_a."PRICE", table_b."PRICE")' in sql


class TestTheWhereClause:
    """The query gives the rows that differ, and nothing else."""

    def test_the_where_clause_joins_each_flag_with_or(self):
        sql = sql_of()
        where_clause = sql.split("WHERE ")[1]

        assert "NOT COALESCE(is_equal_amount, FALSE)" in where_clause
        assert "NOT COALESCE(is_equal_ratio, FALSE)" in where_clause
        assert " OR " in where_clause

    def test_a_flag_of_a_lost_row_counts_as_a_difference(self):
        """A row with no partner gives NULL flags, thus COALESCE reads FALSE."""
        assert "COALESCE(is_equal_amount, FALSE)" in sql_of()

    def test_a_missing_row_reaches_the_where_clause(self):
        where_clause = sql_of().split("WHERE ")[1]

        assert "_clair_row_marker IS NULL" in where_clause

    def test_the_query_limits_the_rows(self):
        assert sql_of().endswith(f"LIMIT {DEFAULT_MAX_DIFFERENCE_ROWS}")

    def test_the_limit_is_a_parameter(self):
        assert sql_of(max_difference_rows=5).endswith("LIMIT 5")


class TestTheResultColumns:
    """A test that fails reads the result, thus it names each column."""

    def test_the_result_names_the_key_then_the_markers_then_the_flags(self):
        _, names = build_comparison_sql(
            ADDRESS_A, ADDRESS_B, ["ID"], COLUMN_TYPES
        )

        assert names[:3] == ["ID", "is_row_in_a", "is_row_in_b"]
        assert "is_equal_amount" in names
        assert "is_equal_ratio" in names

    def test_the_result_holds_the_two_values_of_each_column(self):
        _, names = build_comparison_sql(
            ADDRESS_A, ADDRESS_B, ["ID"], COLUMN_TYPES
        )

        assert "a_amount" in names
        assert "b_amount" in names
        assert "a_ratio" in names
        assert "b_ratio" in names

    def test_the_key_comes_from_the_side_that_holds_the_row(self):
        assert 'COALESCE(table_a."ID", table_b."ID") AS "ID"' in sql_of()
