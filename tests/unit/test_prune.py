"""clair prune drops the objects of one run, and nothing else.

A run names each object that it makes for itself with a suffix that ends with
the run id: ``__clair_<run_id>`` for a staging object, and
``__clair_new_rows_<run_id>`` for the new rows of an Upsert or a DeleteInsert.
A prune must drop exactly these objects, with the DROP of their type.
"""

from __future__ import annotations

from typing import Any, cast

import pytest
from click.testing import CliRunner

import clair
from clair.adapters.base import NamedWarehouseObject
from clair.adapters.objects import StatementStatus, WarehouseObjectType
from clair.adapters.snowflake import SnowflakeAdapter
from clair.cli.main import cli
from clair.core.prune import InvalidRunIdError, PruneSummary
from clair.trouves.address import TrouveAddress
from tests.helpers import RecordingAdapter, make_environment

RUN_ID = "01a1c8936b5175e3b0622ff0eb45d9cc"
OTHER_RUN_ID = "01a1c8936b5175e3b0622ff0eb45d9cd"


def _named(address: str, object_type: WarehouseObjectType = WarehouseObjectType.TABLE) -> NamedWarehouseObject:
    return NamedWarehouseObject(address=TrouveAddress.parse(address), object_type=object_type)


OBJECTS_OF_THE_RUN = [
    _named(f"db.derived.orders__clair_{RUN_ID}"),
    _named(f"db.derived.orders__clair_new_rows_{RUN_ID}"),
    _named(f"db.derived.order_view__clair_{RUN_ID}", WarehouseObjectType.VIEW),
    _named(f"lake.events.clicks__clair_{RUN_ID}", WarehouseObjectType.ICEBERG_TABLE),
]
OTHER_OBJECTS = [
    _named("db.derived.orders"),
    _named(f"db.derived.orders__clair_{OTHER_RUN_ID}"),
    _named(f"db.derived.orders_{RUN_ID}"),
]


def _adapter() -> RecordingAdapter:
    return RecordingAdapter(named_objects=[*OBJECTS_OF_THE_RUN, *OTHER_OBJECTS])


def test_a_prune_drops_each_object_of_the_run_with_the_drop_of_its_type():
    adapter = _adapter()

    summary = clair.prune(env=make_environment(), run_id=RUN_ID, adapter=adapter)

    assert sorted(adapter.record.statements) == sorted(
        [
            f"DROP TABLE IF EXISTS db.derived.orders__clair_{RUN_ID}",
            f"DROP TABLE IF EXISTS db.derived.orders__clair_new_rows_{RUN_ID}",
            f"DROP VIEW IF EXISTS db.derived.order_view__clair_{RUN_ID}",
            f"DROP TABLE IF EXISTS lake.events.clicks__clair_{RUN_ID}",
        ]
    )
    assert summary.failed == []


def test_a_prune_never_touches_a_physical_table_or_another_run():
    adapter = _adapter()

    clair.prune(env=make_environment(), run_id=RUN_ID, adapter=adapter)

    dropped_addresses = {statement.split()[-1] for statement in adapter.record.statements}
    assert dropped_addresses.isdisjoint(str(other.address) for other in OTHER_OBJECTS)
    assert dropped_addresses == {str(pruned.address) for pruned in OBJECTS_OF_THE_RUN}


def test_a_dry_run_gives_the_sql_and_executes_none_of_it():
    adapter = _adapter()

    dry_run = clair.prune(env=make_environment(), run_id=RUN_ID, dry_run=True, adapter=adapter)

    assert adapter.record.statements == []
    assert all(pruned.statement.status == StatementStatus.NOT_RUN for pruned in dry_run.objects)
    assert dry_run.failed == []
    # The SQL of the dry run is the SQL that a prune executes.
    clair.prune(env=make_environment(), run_id=RUN_ID, adapter=adapter)
    assert dry_run.sql == adapter.record.statements
    assert all(f"{sql};" in dry_run.render() for sql in dry_run.sql)


def test_an_upper_case_run_id_finds_the_same_objects():
    summary = clair.prune(env=make_environment(), run_id=RUN_ID.upper(), dry_run=True, adapter=_adapter())

    assert summary.run_id == RUN_ID
    assert len(summary.objects) == len(OBJECTS_OF_THE_RUN)


def test_a_drop_that_fails_is_in_the_summary():
    adapter = RecordingAdapter(named_objects=OBJECTS_OF_THE_RUN, fail_on=["order_view"])

    summary = clair.prune(env=make_environment(), run_id=RUN_ID, adapter=adapter)

    (failed,) = summary.failed
    assert failed.warehouse_object.object_type == WarehouseObjectType.VIEW


@pytest.mark.parametrize("run_id", ["", "abc", RUN_ID[:-1], RUN_ID + "0", "g" * 32, "%" * 32])
def test_a_text_that_is_not_a_run_id_stops_before_the_connection(monkeypatch, run_id):
    # A short or an empty text would match the names of many objects.
    def no_connection(*_: object, **__: object) -> SnowflakeAdapter:
        raise AssertionError("clair connected before it read the run id")

    monkeypatch.setattr(SnowflakeAdapter, "connected", no_connection)

    with pytest.raises(InvalidRunIdError):
        clair.prune(env=make_environment(), run_id=run_id)


class _ShowCursor:
    """A cursor that answers SHOW TABLES and SHOW VIEWS with their own columns."""

    def __init__(self, answers: dict[str, tuple[list[str], list[tuple]]], executed: list[str]) -> None:
        self._answers = answers
        self._executed = executed
        self.description: list[tuple[str]] = []
        self._rows: list[tuple] = []

    def execute(self, sql: str) -> None:
        self._executed.append(sql)
        kind = "TABLES" if sql.startswith("SHOW TABLES") else "VIEWS"
        column_names, self._rows = self._answers[kind]
        self.description = [(name,) for name in column_names]

    def fetchall(self) -> list[tuple]:
        return self._rows

    def close(self) -> None:
        pass


class _ShowConnection:
    def __init__(self, cursor: _ShowCursor) -> None:
        self._cursor = cursor

    def cursor(self) -> _ShowCursor:
        return self._cursor


def test_the_snowflake_search_keeps_the_exact_suffix_and_reads_each_type():
    suffix = f"__clair_{RUN_ID}"
    table_columns = ["created_on", "name", "database_name", "schema_name", "kind", "is_iceberg"]
    view_columns = ["created_on", "name", "reserved", "database_name", "schema_name"]
    answers = {
        "TABLES": (
            table_columns,
            [
                ("t", f"ORDERS__CLAIR_{RUN_ID.upper()}", "DB", "DERIVED", "TABLE", "N"),
                ("t", f"CLICKS__CLAIR_{RUN_ID.upper()}", "LAKE", "EVENTS", "TABLE", "Y"),
                # LIKE treats _ as any character, thus SHOW gives this name too.
                ("t", f"ORDERSXXCLAIRX{RUN_ID.upper()}", "DB", "DERIVED", "TABLE", "N"),
            ],
        ),
        "VIEWS": (view_columns, [("t", f"V__CLAIR_{RUN_ID.upper()}", "", "DB", "DERIVED")]),
    }
    executed: list[str] = []
    adapter = SnowflakeAdapter()
    adapter._conn = cast(Any, _ShowConnection(_ShowCursor(answers, executed)))

    found = adapter.find_objects(suffix)

    assert executed == [
        f"SHOW TABLES LIKE '%{suffix}' IN ACCOUNT",
        f"SHOW VIEWS LIKE '%{suffix}' IN ACCOUNT",
    ]
    assert [(str(item.address).lower(), item.object_type) for item in found] == [
        (f"db.derived.orders__clair_{RUN_ID}", WarehouseObjectType.TABLE),
        (f"lake.events.clicks__clair_{RUN_ID}", WarehouseObjectType.ICEBERG_TABLE),
        (f"db.derived.v__clair_{RUN_ID}", WarehouseObjectType.VIEW),
    ]


class TestThePruneCommand:
    @pytest.fixture(autouse=True)
    def environment(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr("clair.cli.main._load_environment", lambda name: make_environment(name))

    def _give_prune(self, monkeypatch: pytest.MonkeyPatch, fail: bool = False) -> list[bool]:
        calls: list[bool] = []

        def fake_prune(**arguments: Any) -> PruneSummary:
            calls.append(arguments.get("dry_run", False))
            adapter = RecordingAdapter(named_objects=OBJECTS_OF_THE_RUN, fail_on=["order_view"] if fail else [])
            return clair.prune(adapter=adapter, **arguments)

        monkeypatch.setattr("clair.api.prune", fake_prune)
        return calls

    def test_a_dry_run_drops_nothing(self, monkeypatch: pytest.MonkeyPatch):
        calls = self._give_prune(monkeypatch)

        result = CliRunner().invoke(cli, ["prune", "--env", "prod", "--run-id", RUN_ID, "--dry-run"])

        assert result.exit_code == 0
        assert calls == [True]

    def test_yes_drops_with_no_question(self, monkeypatch: pytest.MonkeyPatch):
        calls = self._give_prune(monkeypatch)

        result = CliRunner().invoke(cli, ["prune", "--env", "prod", "--run-id", RUN_ID, "--yes"])

        assert result.exit_code == 0
        assert calls == [True, False]

    def test_a_no_to_the_question_drops_nothing(self, monkeypatch: pytest.MonkeyPatch):
        calls = self._give_prune(monkeypatch)

        result = CliRunner().invoke(cli, ["prune", "--env", "prod", "--run-id", RUN_ID], input="n\n")

        assert result.exit_code == 1
        assert calls == [True]

    def test_a_drop_that_fails_gives_the_status_code_1(self, monkeypatch: pytest.MonkeyPatch):
        self._give_prune(monkeypatch, fail=True)

        result = CliRunner().invoke(cli, ["prune", "--env", "prod", "--run-id", RUN_ID, "--yes"])

        assert result.exit_code == 1

    def test_a_bad_run_id_is_a_usage_error(self):
        result = CliRunner().invoke(cli, ["prune", "--env", "prod", "--run-id", "abc"])

        assert result.exit_code == 2
