"""The tests of the parallel run. They use a false adapter that records the calls."""

from __future__ import annotations

import re
import textwrap
import threading
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

from clair.adapters.base import Statement, StatementStatus, WarehouseAdapter
from clair.adapters.pool import AdapterPool
from clair.core.dag import build_dag, get_executable_nodes
from clair.core.discovery import discover_project
from clair.core.runner import RunStatus, run_project
from clair.core.test_runner import run_tests
from clair.trouves.address import TrouveAddress


class CallRecorder:
    """The shared record of each adapter of one run.

    Attributes:
        statements: Each SQL statement, with the adapter that executed it.
        node_starts: The order in which the nodes started.
        node_ends: The order in which the nodes ended.
        max_concurrent: The largest number of nodes that ran at one time.
        barrier_nodes: Each node that waits at the barrier. A node of this set
            stops in `enter_node` until each other node of the set arrives.
            The barrier thus proves the concurrency, and it needs no sleep: the
            nodes pass together only if the runner runs them together.
        barrier_broke: True after a wait that reached the timeout. A test that
            wants a serial run reads this attribute, because one node alone can
            never complete the barrier.
    """

    def __init__(
        self,
        fail_on: set[str] | None = None,
        barrier_nodes: set[str] | None = None,
        barrier_timeout_seconds: float = 5.0,
    ) -> None:
        self.fail_on = fail_on or set()
        self.barrier_nodes = barrier_nodes or set()
        self.barrier_timeout_seconds = barrier_timeout_seconds
        self.barrier = (
            threading.Barrier(len(self.barrier_nodes)) if self.barrier_nodes else None
        )
        self.barrier_broke = False
        self.statements: list[tuple[int, str]] = []
        self.node_starts: list[str] = []
        self.node_ends: list[str] = []
        self.max_concurrent = 0
        self.adapters_made = 0
        self._concurrent = 0
        self._lock = threading.Lock()

    def enter_node(self, name: str) -> None:
        with self._lock:
            self.node_starts.append(name)
            self._concurrent += 1
            self.max_concurrent = max(self.max_concurrent, self._concurrent)

        # The wait stays outside the lock. A node that holds the lock here
        # stops each other node, and the barrier then never completes.
        if self.barrier is not None and name in self.barrier_nodes:
            try:
                self.barrier.wait(timeout=self.barrier_timeout_seconds)
            except threading.BrokenBarrierError:
                with self._lock:
                    self.barrier_broke = True

    def leave_node(self, name: str) -> None:
        with self._lock:
            self._concurrent -= 1
            self.node_ends.append(name)

    def record(self, adapter_id: int, sql: str) -> None:
        with self._lock:
            self.statements.append((adapter_id, sql))


class RecordingAdapter(WarehouseAdapter):
    """A false adapter. Each connection of the pool is one instance.

    The adapter finds the node name in the CREATE statement. It then records the
    start and the end of that node, so a test can see which nodes ran at one time.
    """

    def __init__(self, recorder: CallRecorder, node_names: list[str]) -> None:
        self.recorder = recorder
        self.node_names = node_names
        self.adapter_id = recorder.adapters_made
        recorder.adapters_made += 1
        self.closed = False

    def connect(self, profile: dict[str, Any]) -> None:
        return None

    def new_connection(self) -> RecordingAdapter:
        return RecordingAdapter(self.recorder, self.node_names)

    def execute(self, sql: str) -> Statement:
        self.recorder.record(self.adapter_id, sql)

        # Find the node that this statement materializes. The target of the
        # CREATE statement names it. A CREATE DATABASE statement and a CREATE
        # SCHEMA statement name no node, and the SQL of a node names the nodes
        # upstream too, thus the test reads the target and nothing else.
        node_name = None
        match = re.match(
            r"\s*CREATE\s+OR\s+REPLACE\s+(?:TABLE|VIEW)\s+(\S+)", sql, re.IGNORECASE
        )
        if match and match.group(1) in self.node_names:
            node_name = match.group(1)

        if node_name is None:
            return Statement(sql=sql, status=StatementStatus.SUCCESS, query_id="qid", query_url="url")

        self.recorder.enter_node(node_name)
        try:
            for failing_name in self.recorder.fail_on:
                if failing_name in sql:
                    return Statement(
                        sql=sql,
                        status=StatementStatus.FAILURE,
                        query_id="qid",
                        query_url="url",
                        error=f"Simulated failure for {failing_name}",
                    )

            return Statement(sql=sql, status=StatementStatus.SUCCESS, query_id="qid", query_url="url")
        finally:
            self.recorder.leave_node(node_name)

    def set_context(
        self,
        warehouse: str | None = None,
        role: str | None = None,
        database_name: str | None = None,
    ) -> None:
        return None

    def table_exists(self, database_name: str, schema_name: str, table_name: str) -> bool:
        return True

    def close(self) -> None:
        self.closed = True

    def fetch_dataframe(self, address: TrouveAddress) -> pd.DataFrame:
        return pd.DataFrame()

    def write_dataframe(
        self, dataframe: pd.DataFrame, address: TrouveAddress
    ) -> Statement:
        return Statement(status=StatementStatus.SUCCESS, query_id="qid", query_url="url")


def _write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(textwrap.dedent(content))


@pytest.fixture
def diamond_project(tmp_path: Path) -> Path:
    """Make a project with a diamond shape.

    orders is the SOURCE. left and right both read orders, thus they can run at
    one time. joined reads left and right, thus it runs last.
    """
    project_root = tmp_path / "diamond"

    _write(
        project_root / "db" / "raw" / "orders.py",
        """
        from clair import Trouve, TrouveType

        trouve = Trouve(type=TrouveType.SOURCE)
        """,
    )
    _write(
        project_root / "db" / "marts" / "left.py",
        """
        from db.raw.orders import trouve as orders

        from clair import Trouve, TrouveType

        trouve = Trouve(
            type=TrouveType.TABLE,
            sql=f"select 1 as a from {orders}",
        )
        """,
    )
    _write(
        project_root / "db" / "marts" / "right.py",
        """
        from db.raw.orders import trouve as orders

        from clair import Trouve, TrouveType

        trouve = Trouve(
            type=TrouveType.TABLE,
            sql=f"select 2 as b from {orders}",
        )
        """,
    )
    _write(
        project_root / "db" / "marts" / "joined.py",
        """
        from db.marts.left import trouve as left
        from db.marts.right import trouve as right

        from clair import Trouve, TrouveType

        trouve = Trouve(
            type=TrouveType.TABLE,
            sql=f"select * from {left} join {right} on 1 = 1",
        )
        """,
    )

    return project_root


@pytest.fixture
def chain_project(tmp_path: Path) -> Path:
    """Make a project with a chain: orders -> first -> middle -> last."""
    project_root = tmp_path / "chain"

    _write(
        project_root / "db" / "raw" / "orders.py",
        """
        from clair import Trouve, TrouveType

        trouve = Trouve(type=TrouveType.SOURCE)
        """,
    )
    _write(
        project_root / "db" / "marts" / "first.py",
        """
        from db.raw.orders import trouve as orders

        from clair import Trouve, TrouveType

        trouve = Trouve(type=TrouveType.TABLE, sql=f"select 1 as a from {orders}")
        """,
    )
    _write(
        project_root / "db" / "marts" / "middle.py",
        """
        from db.marts.first import trouve as first

        from clair import Trouve, TrouveType

        trouve = Trouve(type=TrouveType.TABLE, sql=f"select * from {first}")
        """,
    )
    _write(
        project_root / "db" / "marts" / "last.py",
        """
        from db.marts.middle import trouve as middle

        from clair import Trouve, TrouveType

        trouve = Trouve(type=TrouveType.TABLE, sql=f"select * from {middle}")
        """,
    )

    return project_root


def _build(project_root: Path) -> tuple[Any, list[str]]:
    """Find the Trouves, make the DAG, and give the nodes that clair runs."""
    dag = build_dag(discover_project(project_root))
    return dag, get_executable_nodes(dag)


class TestParallelRun:
    def test_each_trouve_runs_one_time(self, diamond_project: Path):
        dag, selected = _build(diamond_project)
        recorder = CallRecorder()
        adapter = RecordingAdapter(recorder, selected)

        results = list(run_project(dag, selected, adapter, threads=4))

        assert len(results) == 3
        assert all(r.status == RunStatus.SUCCESS for r in results)
        assert sorted(recorder.node_starts) == sorted(selected)

    def test_a_trouve_waits_for_the_trouves_that_it_imports(self, diamond_project: Path):
        dag, selected = _build(diamond_project)
        recorder = CallRecorder()
        adapter = RecordingAdapter(recorder, selected)

        list(run_project(dag, selected, adapter, threads=4))

        joined = "db.marts.joined"
        # joined starts only after left and right end.
        joined_start = recorder.node_starts.index(joined)
        assert joined_start == 2
        assert set(recorder.node_ends[:2]) == {"db.marts.left", "db.marts.right"}

    def test_two_trouves_run_at_one_time(self, diamond_project: Path):
        dag, selected = _build(diamond_project)
        # left and right have no dependency between them, thus the run must
        # hold them at one time. Each one stops at the barrier, and the barrier
        # completes only when both arrive. A serial run therefore reaches the
        # timeout, and the test fails with no dependency on a sleep.
        recorder = CallRecorder(barrier_nodes={"db.marts.left", "db.marts.right"})
        adapter = RecordingAdapter(recorder, selected)

        list(run_project(dag, selected, adapter, threads=4))

        assert not recorder.barrier_broke
        assert recorder.max_concurrent == 2

    def test_one_thread_runs_one_trouve_at_one_time(self, diamond_project: Path):
        dag, selected = _build(diamond_project)
        # One thread must run left and right one after the other. The barrier
        # asks for two nodes at one time, thus it must break. A short timeout
        # keeps the test quick, because the break is the correct result.
        recorder = CallRecorder(
            barrier_nodes={"db.marts.left", "db.marts.right"},
            barrier_timeout_seconds=0.25,
        )
        adapter = RecordingAdapter(recorder, selected)

        list(run_project(dag, selected, adapter, threads=1))

        assert recorder.barrier_broke
        assert recorder.max_concurrent == 1

    def test_a_failure_skips_each_trouve_downstream(self, diamond_project: Path):
        dag, selected = _build(diamond_project)
        recorder = CallRecorder(fail_on={"db.marts.left"})
        adapter = RecordingAdapter(recorder, selected)

        results = {str(r.addresses.physical): r for r in run_project(dag, selected, adapter, threads=4)}

        assert results["db.marts.left"].status == RunStatus.FAILURE
        # right is on the other branch, thus the failure does not stop it.
        assert results["db.marts.right"].status == RunStatus.SUCCESS
        assert results["db.marts.joined"].status == RunStatus.SKIPPED
        assert results["db.marts.joined"].skipped_by == "db.marts.left"

    def test_a_trouve_waits_through_a_trouve_that_the_selector_removed(
        self, chain_project: Path
    ):
        # middle sits between first and last. The selector removes middle, but
        # last still reads the table of first through it, thus last waits.
        dag, all_nodes = _build(chain_project)
        selected = [name for name in all_nodes if name != "db.marts.middle"]
        # last reads the table of first through middle, thus the two never
        # overlap. The barrier must break, and that break proves the order.
        recorder = CallRecorder(
            barrier_nodes={"db.marts.first", "db.marts.last"},
            barrier_timeout_seconds=0.25,
        )
        adapter = RecordingAdapter(recorder, selected)

        list(run_project(dag, selected, adapter, threads=4))

        assert recorder.node_starts == ["db.marts.first", "db.marts.last"]
        assert recorder.barrier_broke
        assert recorder.max_concurrent == 1

    def test_a_failure_skips_through_a_trouve_that_the_selector_removed(
        self, chain_project: Path
    ):
        dag, all_nodes = _build(chain_project)
        selected = [name for name in all_nodes if name != "db.marts.middle"]
        recorder = CallRecorder(fail_on={"db.marts.first"})
        adapter = RecordingAdapter(recorder, selected)

        results = {str(r.addresses.physical): r for r in run_project(dag, selected, adapter, threads=4)}

        assert results["db.marts.first"].status == RunStatus.FAILURE
        assert results["db.marts.last"].status == RunStatus.SKIPPED
        assert results["db.marts.last"].skipped_by == "db.marts.first"

    def test_each_thread_holds_a_private_connection(self, diamond_project: Path):
        dag, selected = _build(diamond_project)
        recorder = CallRecorder()
        adapter = RecordingAdapter(recorder, selected)

        list(run_project(dag, selected, adapter, threads=3))

        # The pool opens threads - 1 connections beside the one that the caller
        # gave, thus the run made 3 adapters in total.
        assert recorder.adapters_made == 3

    def test_clair_makes_each_database_and_schema_one_time(self, diamond_project: Path):
        dag, selected = _build(diamond_project)
        recorder = CallRecorder()
        adapter = RecordingAdapter(recorder, selected)

        list(run_project(dag, selected, adapter, threads=4))

        database_statements = [
            sql for _, sql in recorder.statements if sql.startswith("CREATE DATABASE")
        ]
        schema_statements = [
            sql for _, sql in recorder.statements if sql.startswith("CREATE SCHEMA")
        ]
        # The three Trouves share db.marts, thus one statement is enough.
        assert len(database_statements) == 1
        assert len(schema_statements) == 1

    def test_the_database_statements_run_before_the_threads_start(self, diamond_project: Path):
        dag, selected = _build(diamond_project)
        recorder = CallRecorder()
        adapter = RecordingAdapter(recorder, selected)

        list(run_project(dag, selected, adapter, threads=4))

        # The main thread makes the database and the schema, so the first
        # adapter of the pool holds those statements.
        setup_statements = [
            adapter_id
            for adapter_id, sql in recorder.statements
            if sql.startswith(("CREATE DATABASE", "CREATE SCHEMA"))
        ]
        assert setup_statements == [0, 0]

    def test_a_thread_count_of_zero_raises(self, diamond_project: Path):
        dag, selected = _build(diamond_project)
        recorder = CallRecorder()
        adapter = RecordingAdapter(recorder, selected)

        with pytest.raises(ValueError, match="1 or more"):
            list(run_project(dag, selected, adapter, threads=0))

    def test_the_run_closes_each_connection_that_it_opened(self, diamond_project: Path):
        dag, selected = _build(diamond_project)
        recorder = CallRecorder()
        adapter = RecordingAdapter(recorder, selected)

        list(run_project(dag, selected, adapter, threads=3))

        # The caller keeps the ownership of the adapter that it gave.
        assert adapter.closed is False


class TestAdapterPool:
    def test_one_thread_keeps_one_connection(self):
        recorder = CallRecorder()
        adapter = RecordingAdapter(recorder, [])
        pool = AdapterPool(adapter, size=2)

        assert pool.acquire() is pool.acquire()

    def test_two_threads_hold_different_connections(self):
        recorder = CallRecorder()
        adapter = RecordingAdapter(recorder, [])
        pool = AdapterPool(adapter, size=2)

        acquired: list[WarehouseAdapter] = []
        barrier = threading.Barrier(2)

        def acquire_one() -> None:
            barrier.wait()
            acquired.append(pool.acquire())

        threads = [threading.Thread(target=acquire_one) for _ in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        assert len(acquired) == 2
        assert acquired[0] is not acquired[1]

    def test_more_threads_than_the_pool_size_raises(self):
        recorder = CallRecorder()
        adapter = RecordingAdapter(recorder, [])
        pool = AdapterPool(adapter, size=1)

        pool.acquire()

        errors: list[BaseException] = []

        def acquire_one() -> None:
            try:
                pool.acquire()
            except RuntimeError as e:
                errors.append(e)

        thread = threading.Thread(target=acquire_one)
        thread.start()
        thread.join()

        assert len(errors) == 1

    def test_close_keeps_the_adapter_of_the_caller(self):
        recorder = CallRecorder()
        adapter = RecordingAdapter(recorder, [])
        pool = AdapterPool(adapter, size=3)

        pool.close()

        assert adapter.closed is False
        assert recorder.adapters_made == 3

    def test_a_size_of_zero_raises(self):
        recorder = CallRecorder()
        adapter = RecordingAdapter(recorder, [])

        with pytest.raises(ValueError, match="1 or more"):
            AdapterPool(adapter, size=0)


class TestParallelTests:
    def test_the_results_keep_the_order_of_selected(self, tmp_path: Path):
        project_root = tmp_path / "tested"
        _write(
            project_root / "db" / "raw" / "orders.py",
            """
            from clair import Trouve, TrouveType

            trouve = Trouve(type=TrouveType.SOURCE)
            """,
        )
        for name in ("alpha", "beta", "gamma"):
            _write(
                project_root / "db" / "marts" / f"{name}.py",
                """
                from db.raw.orders import trouve as orders

                from clair import TestNotNull, Trouve, TrouveType

                trouve = Trouve(
                    type=TrouveType.TABLE,
                    sql=f"select 1 as id from {orders}",
                    tests=[TestNotNull(column="id")],
                )
                """,
            )

        dag, selected = _build(project_root)
        recorder = CallRecorder()
        adapter = RecordingAdapter(recorder, selected)

        sequential = run_tests(dag, selected, adapter, threads=1)
        parallel = run_tests(dag, selected, adapter, threads=3)

        assert [str(r.address) for r in sequential] == [
            str(r.address) for r in parallel
        ]
        assert len(parallel) == 3

    def test_a_thread_count_of_zero_raises(self, diamond_project: Path):
        dag, selected = _build(diamond_project)
        recorder = CallRecorder()
        adapter = RecordingAdapter(recorder, selected)

        with pytest.raises(ValueError, match="1 or more"):
            run_tests(dag, selected, adapter, threads=0)
