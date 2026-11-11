"""The test definitions -- the data quality tests that a Trouve holds.

Each test type is a different Pydantic model. Each model has its own checks and
makes its own SQL. The tagged union ``AnyTest`` lets Pydantic build any test
automatically from a ``{"type": "..."}`` dict.
"""

from __future__ import annotations

import re
from abc import ABC, abstractmethod
from enum import StrEnum
from typing import Annotated, ClassVar, Literal

from pydantic import BaseModel, Field, model_validator

from clair.trouves._refs import THIS_PLACEHOLDER


class _ThisSentinel:
    """A marker for the table name of the parent Trouve in a TestSql string.

    In an f-string, this class gives the ``THIS_PLACEHOLDER`` token. Discovery
    replaces the token with the address of the parent Trouve. It uses
    ``_resolve_sql(sql, id_to_logical_address, this_name=logical_address)``.
    """

    def __format__(self, format_spec: str) -> str:
        return THIS_PLACEHOLDER


THIS = _ThisSentinel()
"""A marker for the table name of the parent Trouve in a ``TestSql`` string.

Put it in an f-string to point to the table that the test examines::

    TestSql(sql=f"SELECT * FROM {THIS} WHERE amount < 0")
"""



class TestSeverity(StrEnum):
    """What a test that finds rows does to the run.

    ERROR stops the promotion of the Trouve and skips each Trouve downstream.
    WARN reports the rows, and the run continues as if the test passed. A
    test whose query fails stops the promotion with each severity, because
    clair then knows nothing about the data.
    """

    __test__ = False  # pytest must not collect this class.

    ERROR = "error"
    WARN = "warn"


class Test(BaseModel, ABC):
    """The abstract parent of all the data quality tests.

    Each subclass must declare ``type`` as a Literal string, which is the tag of
    ``AnyTest``. The parent declares no ``type`` field, because a subclass cannot
    narrow a mutable ``str`` field to a Literal. Each subclass must also supply a
    ``to_sql(physical_address)`` method.

    The ``label`` property comes from the class name. Clair removes the ``Test``
    prefix and makes the remainder snake_case.

    Attributes:
        run_with_sample: True runs the test on a sample, and False skips it.
            None takes the default of the class, ``is_run_with_sample_by_default``.
        severity: ERROR stops the promotion when the test finds rows. WARN
            reports the rows, and the run continues.
    """

    # A sample is correct input for a test that cannot fail on a sample when
    # the complete data obeys the test. A row count can fail that way, thus a
    # subclass sets False.
    is_run_with_sample_by_default: ClassVar[bool] = True

    run_with_sample: bool | None = None
    severity: TestSeverity = TestSeverity.ERROR

    @abstractmethod
    def to_sql(self, physical_address: str) -> str:
        """Make the SQL for this test. Zero rows in the result means a pass."""
        ...

    @property
    def is_run_with_sample(self) -> bool:
        """Tell you if clair runs this test on a sample of the data."""
        if self.run_with_sample is not None:
            return self.run_with_sample
        return self.is_run_with_sample_by_default

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

    def to_sql(self, physical_address: str) -> str:
        return (
            f"SELECT {self.column}, COUNT(*) "
            f"FROM {physical_address} "
            f"GROUP BY {self.column} "
            f"HAVING COUNT(*) > 1"
        )


class TestNotNull(Test):
    """A test that a column has no NULL value."""

    type: Literal["not_null"] = "not_null"
    column: str

    def to_sql(self, physical_address: str) -> str:
        return (
            f"SELECT {self.column} "
            f"FROM {physical_address} "
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

    # A row count test needs the complete table, not a sample.
    is_run_with_sample_by_default: ClassVar[bool] = False

    def to_sql(self, physical_address: str) -> str:
        parts = []
        if self.min_rows is not None:
            parts.append(f"SELECT 1 FROM {physical_address} HAVING COUNT(*) < {self.min_rows}")
        if self.max_rows is not None:
            parts.append(f"SELECT 1 FROM {physical_address} HAVING COUNT(*) > {self.max_rows}")
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

    def to_sql(self, physical_address: str) -> str:
        cols = ", ".join(self.columns)
        return (
            f"SELECT {cols}, COUNT(*) "
            f"FROM {physical_address} "
            f"GROUP BY {cols} "
            f"HAVING COUNT(*) > 1"
        )


class TestSql(Test):
    """Your own SQL test on the parent Trouve. Zero rows means a pass.

    Put ``{THIS}`` in an f-string to point to the table name of the parent
    Trouve. Put ``{other_trouve}`` to point to a different Trouve, as in
    ``Trouve.sql``::

        from clair import THIS
        from db.schema.customers import trouve as customers

        TestSql(sql=f"SELECT * FROM {THIS} t LEFT JOIN {customers} c ON t.cid = c.id WHERE c.id IS NULL")

    ``sql`` keeps the tokens that the f-string makes. Discovery writes the SQL
    with the true addresses to ``resolved_sql``, and the test runner executes
    that. The two fields are the counterpart of ``Trouve.sql`` and
    ``CompiledAttributes.resolved_sql``.
    """

    type: Literal["sql"] = "sql"
    sql: str
    # Discovery sets this from sql. recompile_for_selection() sets it a second
    # time, because the selection decides the address of each upstream Trouve.
    resolved_sql: str = ""

    # Your own SQL can group rows or read many tables, so a sample is not safe.
    is_run_with_sample_by_default: ClassVar[bool] = False

    def to_sql(self, physical_address: str) -> str:
        return self.resolved_sql


AnyTest = Annotated[
    TestUnique | TestNotNull | TestRowCount | TestUniqueColumns | TestSql,
    Field(discriminator="type"),
]
