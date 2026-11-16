"""A TestSql reads the object that the plan tests, through {THIS}.

A staged run tests the candidate at the staging address, and promotes it after
the tests pass. A TestSql that read the physical address would test the old
data, and the promotion would then trust a test of the wrong object.

The resolved text of a TestSql holds no address of its own Trouve, thus two
Trouves can share one TestSql object, and each one tests its own object.
"""

from __future__ import annotations

import pytest

import clair
from tests.helpers import RecordingAdapter, make_environment, write_project_files

SHARED_TESTS = (
    "from clair import THIS, TestSql\n"
    'NO_NEGATIVE = TestSql(sql=f"select * from {THIS} where amount < 0")\n'
)


def _trouve_file(amount: int) -> str:
    return (
        "from clair import Table\n"
        "from _shared.tests import NO_NEGATIVE\n"
        f'trouve = Table(sql="select {amount} as amount", tests=[NO_NEGATIVE])\n'
    )


@pytest.fixture
def shared_test_project(tmp_path):
    return write_project_files(
        tmp_path,
        {
            "_shared/tests.py": SHARED_TESTS,
            "db/s/alpha.py": _trouve_file(1),
            "db/s/beta.py": _trouve_file(2),
        },
    )


def _test_sql_of(compile_output, address: str) -> str:
    node = compile_output.node(address)
    assert node is not None
    (test_statement,) = [sql for sql in node.sql if "amount < 0" in sql]
    return test_statement.splitlines()[-1]


@pytest.mark.parametrize("address", ["db.s.alpha", "db.s.beta"])
def test_a_staged_plan_tests_the_staging_object_of_each_owner(shared_test_project, address):
    output = clair.compile(shared_test_project, env="dev", use_staging=True)

    node = output.node(address)
    assert node is not None and node.addresses.staging is not None
    assert _test_sql_of(output, address) == (
        f"select * from {node.addresses.staging} where amount < 0"
    )


def test_a_staged_run_sends_the_test_against_the_candidate(shared_test_project):
    adapter = RecordingAdapter()

    summary = clair.run(shared_test_project, env=make_environment(), adapter=adapter, threads=1)

    assert len(summary.succeeded) == 2
    for result in summary.succeeded:
        (test_result,) = result.test_results
        assert test_result.statement.sql == (
            f"select * from {result.addresses.staging} where amount < 0"
        )


def test_clair_test_reads_the_physical_object_of_each_owner(shared_test_project):
    adapter = RecordingAdapter()

    summary = clair.test(shared_test_project, env=make_environment(), adapter=adapter, threads=1)

    assert sorted(result.statement.sql for result in summary.results) == [
        "select * from db.s.alpha where amount < 0",
        "select * from db.s.beta where amount < 0",
    ]
