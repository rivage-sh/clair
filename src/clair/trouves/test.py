"""Test definitions -- data quality tests attached to Trouves.

Each test type is a separate Pydantic model with its own validation and
SQL generation logic. The discriminated union ``AnyTest`` lets Pydantic
deserialize any test from a ``{"type": "..."}`` dict automatically.
"""

from __future__ import annotations

import re
from abc import ABC, abstractmethod
from typing import Annotated, Literal

from pydantic import BaseModel, Field, model_validator

from clair.trouves._refs import THIS_PLACEHOLDER


class _ThisSentinel:
    """Sentinel for the owning Trouve's table name in TestSql SQL strings.

    When used in an f-string, emits ``THIS_PLACEHOLDER`` as a placeholder.
    Discovery resolves this token to the owning Trouve's full name via
    ``_resolve_sql(sql, id_to_full_name, this_name=logical_name)``.
    """

    def __format__(self, format_spec: str) -> str:
        return THIS_PLACEHOLDER


THIS = _ThisSentinel()
"""Sentinel for the owning Trouve's table name inside ``TestSql`` SQL strings.

Use in an f-string to reference the table being tested::

    TestSql(sql=f"SELECT * FROM {THIS} WHERE amount < 0")
"""



class Test(BaseModel, ABC):
    """Abstract base for all data quality tests.

    Every concrete subclass must define ``type`` as a Literal string
    (used as discriminator) and implement ``to_sql(full_name)``.

    The ``label`` property is derived automatically from the class name:
    the ``Test`` prefix is stripped and the remainder converted to snake_case.
    """

    type: str

    @abstractmethod
    def to_sql(self, full_name: str) -> str:
        """Generate the SQL for this test. Zero returned rows means pass."""
        ...

    @property
    def is_run_with_sample(self) -> bool:
        """Whether this test should run when native sampling is active.

        Most tests are valid on sampled data. Override to return False for
        tests (like row counts) whose results are meaningless on a subset.
        """
        return True

    @property
    def label(self) -> str:
        """Human-readable label derived from the class name (sans 'Test' prefix)."""
        name = type(self).__name__
        if name.startswith("Test"):
            name = name[4:]
        return re.sub(r"(?<!^)(?=[A-Z])", "_", name).lower()


class TestUnique(Test):
    """Assert that a column contains no duplicate values."""

    type: Literal["unique"] = "unique"
    column: str

    def to_sql(self, full_name: str) -> str:
        return (
            f"SELECT {self.column}, COUNT(*) "
            f"FROM {full_name} "
            f"GROUP BY {self.column} "
            f"HAVING COUNT(*) > 1"
        )


class TestNotNull(Test):
    """Assert that a column contains no NULL values."""

    type: Literal["not_null"] = "not_null"
    column: str

    def to_sql(self, full_name: str) -> str:
        return (
            f"SELECT {self.column} "
            f"FROM {full_name} "
            f"WHERE {self.column} IS NULL"
        )



class TestRowCount(Test):
    """Assert that the table row count falls within the given bounds.

    At least one of ``min_rows`` or ``max_rows`` must be set.
    """

    type: Literal["row_count"] = "row_count"
    min_rows: int | None = None
    max_rows: int | None = None

    @model_validator(mode="after")
    def _validate_bounds(self) -> TestRowCount:
        if self.min_rows is None and self.max_rows is None:
            raise ValueError("at least one of min_rows or max_rows must be set")
        if self.min_rows is not None and self.min_rows < 0:
            raise ValueError("min_rows must be >= 0")
        if self.max_rows is not None and self.min_rows is not None and self.max_rows < self.min_rows:
            raise ValueError("max_rows must be >= min_rows")
        return self

    @property
    def is_run_with_sample(self) -> bool:
        """Row count tests are meaningless on sampled data."""
        return False

    def to_sql(self, full_name: str) -> str:
        parts = []
        if self.min_rows is not None:
            parts.append(f"SELECT 1 FROM {full_name} HAVING COUNT(*) < {self.min_rows}")
        if self.max_rows is not None:
            parts.append(f"SELECT 1 FROM {full_name} HAVING COUNT(*) > {self.max_rows}")
        return " UNION ALL ".join(parts)


class TestUniqueColumns(Test):
    """Assert that a combination of columns is unique across all rows."""

    type: Literal["unique_columns"] = "unique_columns"
    columns: list[str]

    @model_validator(mode="after")
    def _validate_columns(self) -> TestUniqueColumns:
        if len(self.columns) < 2:
            raise ValueError(
                "unique_columns test requires columns with at least 2 entries"
            )
        return self

    def to_sql(self, full_name: str) -> str:
        cols = ", ".join(self.columns)
        return (
            f"SELECT {cols}, COUNT(*) "
            f"FROM {full_name} "
            f"GROUP BY {cols} "
            f"HAVING COUNT(*) > 1"
        )


class TestSql(Test):
    """Arbitrary SQL test against the owning Trouve. Zero returned rows = pass.

    Use ``{THIS}`` in an f-string to reference the owning Trouve's table name.
    Use ``{other_trouve}`` to reference other Trouves (same as ``Trouve.sql``).
    Discovery resolves cross-Trouve placeholder tokens; ``{THIS}`` is substituted
    at test execution time via ``to_sql(full_name)``::

        from clair import THIS
        from db.schema.customers import trouve as customers

        TestSql(sql=f"SELECT * FROM {THIS} t LEFT JOIN {customers} c ON t.cid = c.id WHERE c.id IS NULL")
    """

    type: Literal["sql"] = "sql"
    sql: str

    @property
    def is_run_with_sample(self) -> bool:
        """Custom SQL may aggregate or span tables — skip during sample runs."""
        return False

    def to_sql(self, full_name: str) -> str:
        return self.sql


AnyTest = Annotated[
    TestUnique | TestNotNull | TestRowCount | TestUniqueColumns | TestSql,
    Field(discriminator="type"),
]
