"""The test definitions -- the data quality tests that a Trouve holds.

Each test type is a different Pydantic model. Each model has its own checks and
makes its own SQL. The tagged union ``AnyTest`` lets Pydantic build any test
automatically from a ``{"type": "..."}`` dict.
"""

from __future__ import annotations

import re
from abc import ABC, abstractmethod
from typing import Annotated, Literal

from pydantic import BaseModel, Field, model_validator

from clair.trouves._refs import THIS_PLACEHOLDER


class _ThisSentinel:
    """A marker for the table name of the parent Trouve in a TestSql string.

    In an f-string, this class gives the ``THIS_PLACEHOLDER`` token. Discovery
    replaces the token with the full name of the parent Trouve. It uses
    ``_resolve_sql(sql, id_to_full_name, this_name=logical_name)``.
    """

    def __format__(self, format_spec: str) -> str:
        return THIS_PLACEHOLDER


THIS = _ThisSentinel()
"""A marker for the table name of the parent Trouve in a ``TestSql`` string.

Put it in an f-string to point to the table that the test examines::

    TestSql(sql=f"SELECT * FROM {THIS} WHERE amount < 0")
"""



class Test(BaseModel, ABC):
    """The abstract parent of all the data quality tests.

    Each subclass must set ``type`` to a Literal string, which is the tag of the
    union. Each subclass must also supply a ``to_sql(full_name)`` method.

    The ``label`` property comes from the class name. Clair removes the ``Test``
    prefix and makes the remainder snake_case.
    """

    type: str

    @abstractmethod
    def to_sql(self, full_name: str) -> str:
        """Make the SQL for this test. Zero rows in the result means a pass."""
        ...

    @property
    def is_run_with_sample(self) -> bool:
        """Tell you if clair can run this test on a sample of the data.

        A sample is correct input for most tests. Override this property and
        give False for a test that needs the complete table, such as a row
        count test.
        """
        return True

    @property
    def label(self) -> str:
        """A label for a person to read. It comes from the class name."""
        name = type(self).__name__
        name = name.removeprefix("Test")
        return re.sub(r"(?<!^)(?=[A-Z])", "_", name).lower()


class TestUnique(Test):
    """A test that a column has no duplicate value."""

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
    """A test that a column has no NULL value."""

    type: Literal["not_null"] = "not_null"
    column: str

    def to_sql(self, full_name: str) -> str:
        return (
            f"SELECT {self.column} "
            f"FROM {full_name} "
            f"WHERE {self.column} IS NULL"
        )



class TestRowCount(Test):
    """A test that the row count of the table stays between two limits.

    You must set ``min_rows`` or ``max_rows``, or both.
    """

    type: Literal["row_count"] = "row_count"
    min_rows: int | None = None
    max_rows: int | None = None

    @model_validator(mode="after")
    def _validate_bounds(self) -> TestRowCount:
        if self.min_rows is None and self.max_rows is None:
            raise ValueError("you must set min_rows or max_rows, or both")
        if self.min_rows is not None and self.min_rows < 0:
            raise ValueError("min_rows must be >= 0")
        if self.max_rows is not None and self.min_rows is not None and self.max_rows < self.min_rows:
            raise ValueError("max_rows must be >= min_rows")
        return self

    @property
    def is_run_with_sample(self) -> bool:
        """A row count test needs the complete table, not a sample."""
        return False

    def to_sql(self, full_name: str) -> str:
        parts = []
        if self.min_rows is not None:
            parts.append(f"SELECT 1 FROM {full_name} HAVING COUNT(*) < {self.min_rows}")
        if self.max_rows is not None:
            parts.append(f"SELECT 1 FROM {full_name} HAVING COUNT(*) > {self.max_rows}")
        return " UNION ALL ".join(parts)


class TestUniqueColumns(Test):
    """A test that a set of columns has a different value in each row."""

    type: Literal["unique_columns"] = "unique_columns"
    columns: list[str]

    @model_validator(mode="after")
    def _validate_columns(self) -> TestUniqueColumns:
        if len(self.columns) < 2:
            raise ValueError(
                "the unique_columns test needs a minimum of 2 columns"
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
    """Your own SQL test on the parent Trouve. Zero rows means a pass.

    Put ``{THIS}`` in an f-string to point to the table name of the parent
    Trouve. Put ``{other_trouve}`` to point to a different Trouve, as in
    ``Trouve.sql``. Discovery replaces each token that points to a different
    Trouve. ``to_sql(full_name)`` replaces ``{THIS}`` when the test runs::

        from clair import THIS
        from db.schema.customers import trouve as customers

        TestSql(sql=f"SELECT * FROM {THIS} t LEFT JOIN {customers} c ON t.cid = c.id WHERE c.id IS NULL")
    """

    type: Literal["sql"] = "sql"
    sql: str

    @property
    def is_run_with_sample(self) -> bool:
        """Your own SQL can group rows or read many tables, so a sample is not safe."""
        return False

    def to_sql(self, full_name: str) -> str:
        return self.sql


AnyTest = Annotated[
    TestUnique | TestNotNull | TestRowCount | TestUniqueColumns | TestSql,
    Field(discriminator="type"),
]
