"""A reference in a TestSql is an edge of the DAG, of the kind TEST.

The test of a Trouve runs after its build, and it can read another Trouve. The
run must build that Trouve first, or the test reads an old object, or no
object. Thus the run treats a TEST edge as a READ edge: the dependent waits,
and a failure upstream skips it.
"""

from __future__ import annotations

from pathlib import Path

import clair
from clair.core.dag import EdgeKind, build_dag
from clair.core.discovery import discover_project
from clair.core.runner import RunSkip
from tests.helpers import RecordingAdapter, make_environment, write_project_files

ORDERS_TEST = (
    "from clair import THIS, Table, TestSql\n"
    "from db.s.customers import trouve as customers\n"
    "trouve = Table(\n"
    '    sql="select 1 as customer_id",\n'
    '    tests=[TestSql(sql=f"select * from {THIS} o left join {customers} c '
    'on o.customer_id = c.customer_id where c.customer_id is null")],\n'
    ")\n"
)


def _edge_kinds(project_root: Path) -> dict[tuple[str, str], EdgeKind]:
    dag = build_dag(discover_project(project_root))
    return {(source, target): kind for source, target, kind in dag.edges(data="kind")}


def test_a_reference_in_a_test_sql_adds_a_test_edge(tmp_path):
    project_root = write_project_files(
        tmp_path,
        {
            "db/s/customers.py": 'from clair import Table\ntrouve = Table(sql="select 1 as customer_id")\n',
            "db/s/orders.py": ORDERS_TEST,
        },
    )

    assert _edge_kinds(project_root) == {("db.s.customers", "db.s.orders"): EdgeKind.TEST}


def test_a_read_edge_wins_against_a_test_edge(tmp_path):
    orders_reads_customers = ORDERS_TEST.replace(
        'sql="select 1 as customer_id"', 'sql=f"select customer_id from {customers}"'
    )
    project_root = write_project_files(
        tmp_path,
        {
            "db/s/customers.py": 'from clair import Table\ntrouve = Table(sql="select 1 as customer_id")\n',
            "db/s/orders.py": orders_reads_customers,
        },
    )

    assert _edge_kinds(project_root) == {("db.s.customers", "db.s.orders"): EdgeKind.READ}


def test_a_source_gets_no_test_edge(tmp_path):
    # clair runs no test of a Source. An edge into a Source would make each
    # Trouve below the Source wait for the Trouve of the test.
    source_with_test = (
        "from clair import THIS, Source, TestSql\n"
        "from db.s.customers import trouve as customers\n"
        'trouve = Source(tests=[TestSql(sql=f"select * from {THIS} join {customers} using (id)")])\n'
    )
    project_root = write_project_files(
        tmp_path,
        {
            "db/s/customers.py": 'from clair import Table\ntrouve = Table(sql="select 1 as id")\n',
            "db/raw/events.py": source_with_test,
        },
    )

    assert _edge_kinds(project_root) == {}


def test_a_failure_of_the_test_dependency_skips_the_dependent(tmp_path):
    project_root = write_project_files(
        tmp_path,
        {
            "db/s/customers.py": 'from clair import Table\ntrouve = Table(sql="select 1 as customer_id")\n',
            "db/s/orders.py": ORDERS_TEST,
        },
    )
    adapter = RecordingAdapter(fail_on=["CREATE OR REPLACE TABLE db.s.customers__clair_"])

    summary = clair.run(project_root, env=make_environment(), adapter=adapter, threads=1)

    orders_result = summary.result("db.s.orders")
    assert isinstance(orders_result, RunSkip)
    assert orders_result.skipped_by == "db.s.customers"
