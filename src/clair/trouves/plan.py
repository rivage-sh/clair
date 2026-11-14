"""The plan of one node: each statement that clair sends to materialize a Trouve.

``Trouve.plan()`` gives a ``NodePlan``. The plan holds one field for each
``clair.phase.Phase``, and the runner executes the phases in this order:

1. ``prepare``: make the target ready. An incremental run clones production to
   the staging address, and adds each new column.
2. ``build``: write the rows.
3. ``configure``: set the properties of the table after the write, such as the
   clustering key.
4. ``test``: the data quality tests. Each row that a test query gives is a
   failure.
5. ``promote``: make production hold the tested data. A staged run only.
6. ``document``: write the docs as Snowflake comments on production.
7. ``retire``: deal with the promoted staging object. A staged run only.
8. ``cleanup``: remove each object that this run made for itself. It runs last,
   also when a statement fails.

Two policy methods of the Trouve decide, and send no SQL:
``effective_mode()`` before the plan, and ``should_promote()`` between the
tests and the promotion.

``plan()`` opens no connection. The runner reads the warehouse first, and gives
what it read to ``plan()`` in a ``PlanContext``. Thus ``clair compile`` and
``clair validate`` make the same plan with no connection.
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, computed_field

from clair.adapters.objects import Statement, WarehouseColumn, WarehouseObject
from clair.phase import Phase
from clair.trouves.address import TrouveAddress
from clair.trouves.run_config import RunMode
from clair.trouves.statements import WarehouseComments
from clair.trouves.test import Test, TestSeverity


class PlanContext(BaseModel):
    """The facts of one node that ``plan()`` reads.

    Attributes:
        run_id: The identifier of this run. Put it in the name of each object
            that the plan makes for this run only.
        effective_mode: What ``effective_mode()`` gave: ``FULL_REFRESH`` or
            ``INCREMENTAL``.
        staged: True when the run tests a staging object before the promotion.
            False for ``--no-test``: the build then writes production directly,
            and clair calls neither ``promote()`` nor ``retire()``.
        offline: True in ``clair compile`` and ``clair validate``. Clair read
            nothing from the warehouse, thus ``current_object``,
            ``target_columns`` and ``current_comments`` are None.
        physical_address: The production address.
        staging_address: The address of the candidate. None when ``staged`` is
            False.
        current_object: The object at the physical address before the run.
            None when the address holds nothing, and offline.
        target_columns: The columns of the production table, in the order of
            the warehouse. An incremental run only.
        added_columns: The columns that the SQL gives, and the production
            table does not have.
        current_comments: The comments of the production object before the
            run. None means a new object, or offline.
    """

    model_config = ConfigDict(frozen=True)

    run_id: str
    effective_mode: RunMode
    staged: bool
    offline: bool = False
    physical_address: TrouveAddress
    staging_address: TrouveAddress | None = None
    current_object: WarehouseObject | None = None
    target_columns: list[WarehouseColumn] | None = None
    added_columns: list[WarehouseColumn] = []
    current_comments: WarehouseComments | None = None

    @property
    def target_address(self) -> TrouveAddress:
        """The address that ``prepare``, ``build``, ``configure`` and ``test`` write and read.

        The staging address of a staged run, and the physical address of a run
        with ``--no-test``.
        """
        return self.staging_address or self.physical_address

    @property
    def target_column_names(self) -> list[str] | None:
        """The columns of the target after ``prepare``: each column of production, then each new column.

        None in a full refresh, and offline.
        """
        if self.target_columns is None:
            return None
        return [
            *(column.name for column in self.target_columns),
            *(column.name for column in self.added_columns),
        ]


class TestQuery(BaseModel):
    """One data quality test as SQL. Each row of the result is a failure.

    Attributes:
        test_index: The index of the test in the test list of the Trouve.
        label: The name of the test for a person to read, such as ``unique``.
        column_name: The column that the test examines, or None.
        severity: ERROR stops the promotion, and WARN does not.
        run_with_sample: False makes a sampled run skip the test.
        sql: The query.
    """

    __test__ = False  # pytest must not collect this class.

    model_config = ConfigDict(frozen=True)

    test_index: int
    label: str
    column_name: str | None = None
    severity: TestSeverity = TestSeverity.ERROR
    run_with_sample: bool = True
    sql: str

    @classmethod
    def of(cls, test: Test, test_index: int, address: TrouveAddress) -> TestQuery:
        """Give the query of one test of the Trouve, against the object at *address*."""
        return cls(
            test_index=test_index,
            label=test.label,
            column_name=getattr(test, "column", None),
            severity=test.severity,
            run_with_sample=test.is_run_with_sample,
            sql=test.to_sql(str(address)),
        )


class TestResult(BaseModel):
    """The result of one data quality test.

    The test query gives the rows that disobey the test condition. Thus the
    statement holds the count, the query ID and the error, and this object needs
    no copy of them.

    Attributes:
        address: The address of the Trouve that the test examines.
        test_index: The index of this test in the test list of the Trouve. The
            first index is 0.
        test_type: A label for a person to read, such as "unique", "not_null",
            or "sql".
        column_name: The column that the test examines. It is None for a test
            on the full table.
        statement: The test query, and what the warehouse answered.
        severity: The severity of the test.
    """

    __test__ = False  # pytest must not collect this class.

    address: TrouveAddress
    test_index: int
    test_type: str
    column_name: str | None
    statement: Statement
    severity: TestSeverity = TestSeverity.ERROR

    @computed_field  # type: ignore[prop-decorator]
    @property
    def passed(self) -> bool:
        """True if the test query gave zero rows. Thus the data is correct."""
        return self.statement.success and self.statement.row_count == 0

    @computed_field  # type: ignore[prop-decorator]
    @property
    def is_blocking(self) -> bool:
        """True if this result stops the promotion and fails the command.

        A test that passed never blocks. A WARN test that found rows does not
        block. A query that failed blocks with each severity.
        """
        if self.passed:
            return False
        if self.error:
            return True
        return self.severity == TestSeverity.ERROR

    @property
    def failing_row_count(self) -> int:
        """The number of rows that disobey the test condition."""
        return self.statement.row_count if self.statement.success else 0

    @property
    def error(self) -> str:
        """The error message if the query itself did not execute."""
        return self.statement.error


class NodePlan(BaseModel):
    """Each statement of one node, by phase.

    The runner executes the phases in the order of the fields. A phase can be
    empty. See the module docstring for what each phase does, and for when the
    runner executes it.
    """

    model_config = ConfigDict(frozen=True)

    prepare: list[str] = []
    build: list[str] = []
    configure: list[str] = []
    test: list[TestQuery] = []
    promote: list[str] = []
    document: list[str] = []
    retire: list[str] = []
    cleanup: list[str] = []

    def statements_of(self, phase: Phase) -> list[str]:
        """Give the SQL of one phase. For ``Phase.TEST``, give the SQL of each test query."""
        if phase == Phase.TEST:
            return [query.sql for query in self.test]
        return list(getattr(self, phase.value))

    @property
    def write_statements(self) -> list[str]:
        """The statements before the tests: ``prepare``, ``build`` and ``configure``."""
        return [*self.prepare, *self.build, *self.configure]
