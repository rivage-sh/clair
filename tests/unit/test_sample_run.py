"""The tests of a sampled run and a sampled test run: ``clair.run(sample=True)``, ``clair.test(sample=True)``.

Each test drives the API against a project on the disk, with a
``RecordingAdapter``. The routing entry of "dev" sends each TABLE to a ``dev_``
database, and keeps each SOURCE. The project holds:

* ``raw.shop.orders``, a Source with no sample_sql. A sample reads a row limit.
* ``raw.shop.customers``, a Source whose sample_sql reads the customers of a
  seed, ``ref.shop.test_customers``.
* ``raw.shop.countries``, a Source whose sample_sql reads each row.
* ``mart.shop.order_facts``, a Table that joins the three sources.
* ``mart.shop.daily_orders``, a Table that reads order_facts.
* ``mart.shop.order_frame``, a PandasTable that reads orders and countries.
"""

from __future__ import annotations

import textwrap
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

import clair
from clair.core.discovery import discover_project
from clair.core.runner import RunSummary
from clair.exceptions import CompileError, SampleTargetsProductionError
from clair.trouves.test import TestNotNull
from clair.trouves.trouve import DEFAULT_SAMPLE_ROWS, Source, Table
from tests.helpers import RecordingAdapter, make_environment

ORDERS_SAMPLE = f"(SELECT * FROM raw.shop.orders LIMIT {DEFAULT_SAMPLE_ROWS})"
CUSTOMERS_SAMPLE = (
    "(SELECT * FROM raw.shop.customers WHERE customer_id IN "
    "(SELECT customer_id FROM dev_ref.shop.test_customers))"
)
COUNTRIES_SAMPLE = "(SELECT * FROM raw.shop.countries)"
# A selector matches the physical address, and "dev" adds a prefix to the database.
ORDER_FRAME_SELECTOR = "*.shop.order_frame"
DAILY_ORDERS_SELECTOR = "*.shop.daily_orders"

ROUTING_FILE = """
from clair import RoutingEntry, RoutingTable, TrouveAddress, TrouveType


class DevRouting(RoutingEntry):
    environment_name: str = "dev"

    def route(self, trouve_address: TrouveAddress, trouve_type: TrouveType) -> TrouveAddress:
        if trouve_type == TrouveType.SOURCE:
            return trouve_address
        return trouve_address.model_copy(
            update={"database_name": f"dev_{trouve_address.database_name}"}
        )


routing = RoutingTable(entries=[DevRouting()])
"""

CUSTOMERS_FILE = '''
from ref.shop.test_customers import trouve as test_customers

from clair import THIS, Source

trouve = Source(
    sample_sql=f"SELECT * FROM {THIS} WHERE customer_id IN (SELECT customer_id FROM {test_customers})",
)
'''

TEST_CUSTOMERS_FILE = """
import pandas as pd

from clair import SeedTable

trouve = SeedTable(dataframe=pd.DataFrame({"customer_id": [7, 9]}))
"""

COUNTRIES_FILE = """
from clair import THIS, Source

trouve = Source(sample_sql=f"SELECT * FROM {THIS}")
"""


def order_facts_file(orders_reference: str) -> str:
    """Give a Table that joins the three sources.

    *orders_reference* is the f-string expression of the orders reference, for
    example ``{orders} AS o``.
    """
    return f'''
from raw.shop.countries import trouve as countries
from raw.shop.customers import trouve as customers
from raw.shop.orders import trouve as orders

from clair import Table, TestNotNull, TestRowCount, TestUnique

trouve = Table(
    sql=f"""
        SELECT o.order_id, o.created_at, c.customer_name, k.country_name
        FROM {orders_reference}
        JOIN {{customers}} AS c ON o.customer_id = c.customer_id
        JOIN {{countries}} AS k ON c.country_code = k.country_code
    """,
    tests=[
        TestNotNull(column="order_id"),
        TestRowCount(min_rows=1),
        TestUnique(column="order_id", run_with_sample=False),
    ],
)
'''


DAILY_ORDERS_FILE = '''
from mart.shop.order_facts import trouve as order_facts

from clair import THIS, Table, TestNotNull

trouve = Table(
    sql=f"SELECT DATE(created_at) AS order_date, COUNT(*) AS order_count FROM {order_facts} GROUP BY 1",
    sample_sql=f"SELECT * FROM {THIS} WHERE order_date >= '2024-07-01'",
    tests=[TestNotNull(column="order_date")],
)
'''

ORDER_FRAME_FILE = '''
import pandas as pd
from raw.shop.countries import trouve as countries
from raw.shop.orders import trouve as orders

from clair import PandasTable


def join_countries(orders: pd.DataFrame, countries: pd.DataFrame) -> pd.DataFrame:
    return orders.merge(countries, on="country_code")


trouve = PandasTable(transform=join_countries, inputs=[orders, countries])
'''


def write_project(project_root: Path, orders_reference: str = "{orders} AS o") -> Path:
    files = {
        "__routing__.py": ROUTING_FILE,
        "raw/shop/orders.py": "from clair import Source\n\ntrouve = Source()\n",
        "raw/shop/customers.py": CUSTOMERS_FILE,
        "raw/shop/countries.py": COUNTRIES_FILE,
        "ref/shop/test_customers.py": TEST_CUSTOMERS_FILE,
        "mart/shop/order_facts.py": order_facts_file(orders_reference),
        "mart/shop/daily_orders.py": DAILY_ORDERS_FILE,
        "mart/shop/order_frame.py": ORDER_FRAME_FILE,
    }
    for relative_path, text in files.items():
        file_path = project_root / relative_path
        file_path.parent.mkdir(parents=True, exist_ok=True)
        file_path.write_text(textwrap.dedent(text))
    return project_root


@pytest.fixture
def project(tmp_path: Path) -> Path:
    return write_project(tmp_path / "shop_project")


def recording_adapter() -> RecordingAdapter:
    """Give an adapter that answers each DataFrame read with one row."""
    return RecordingAdapter(
        default_dataframe=pd.DataFrame(
            {"order_id": [1], "country_code": ["FR"], "customer_id": [7]}
        )
    )


def run_sampled(
    project: Path, sample: bool = True, **options: Any
) -> tuple[RunSummary, RecordingAdapter]:
    adapter = recording_adapter()
    summary = clair.run(
        project, env=make_environment("dev"), adapter=adapter, sample=sample, **options
    )
    return summary, adapter


def sql_of(summary: RunSummary, address: str) -> str:
    """Give the build SQL of one Trouve: each statement, joined."""
    return "\n".join(statement.sql for statement in summary.result(address).statements)


class TestASampledReference:
    def test_a_trouve_without_a_sample_sql_gives_the_row_limit(self, project: Path):
        summary, _ = run_sampled(project)

        assert f"FROM {ORDERS_SAMPLE} AS o" in sql_of(summary, "mart.shop.order_facts")

    def test_sample_rows_sets_the_row_limit(self, project: Path):
        summary, _ = run_sampled(project, sample_rows=25)

        assert "(SELECT * FROM raw.shop.orders LIMIT 25) AS o" in sql_of(
            summary, "mart.shop.order_facts"
        )

    def test_a_sample_sql_reads_this_and_the_trouves_that_it_names(self, project: Path):
        """THIS gives the address of the Source, and the seed reads at its run address."""
        summary, _ = run_sampled(project)

        assert f"JOIN {CUSTOMERS_SAMPLE} AS c" in sql_of(summary, "mart.shop.order_facts")

    def test_a_sample_sql_can_read_each_row(self, project: Path):
        summary, _ = run_sampled(project)

        assert f"JOIN {COUNTRIES_SAMPLE} AS k" in sql_of(summary, "mart.shop.order_facts")

    def test_a_reference_to_a_trouve_of_the_run_keeps_the_address(self, project: Path):
        """A Trouve of the run is a sample already. A second sample would make it smaller."""
        summary, _ = run_sampled(project)

        assert "FROM dev_mart.shop.order_facts GROUP BY 1" in sql_of(
            summary, "mart.shop.daily_orders"
        )

    def test_a_reference_to_a_trouve_outside_the_selection_reads_its_sample(
        self, project: Path
    ):
        summary, _ = run_sampled(project, select=[DAILY_ORDERS_SELECTOR])

        assert (
            f"FROM (SELECT * FROM mart.shop.order_facts LIMIT {DEFAULT_SAMPLE_ROWS}) GROUP BY 1"
            in sql_of(summary, "mart.shop.daily_orders")
        )

    def test_a_run_without_a_sample_reads_each_row(self, project: Path):
        summary, _ = run_sampled(project, sample=False)

        build_sql = sql_of(summary, "mart.shop.order_facts")
        assert "FROM raw.shop.orders AS o" in build_sql
        assert "LIMIT" not in build_sql
        assert summary.sample_rows is None

    def test_the_summary_holds_the_row_limit(self, project: Path):
        summary, _ = run_sampled(project, sample_rows=25)

        assert summary.sample_rows == 25

    def test_a_column_that_a_sampled_reference_qualifies_stops_the_run(self, tmp_path: Path):
        project = write_project(
            tmp_path / "shop_project", orders_reference="{orders} AS o, {orders}.order_id"
        )

        with pytest.raises(CompileError, match="alias"):
            run_sampled(project)

    @pytest.mark.parametrize("sample_rows", [0, -1])
    def test_a_row_limit_below_one_raises(self, project: Path, sample_rows: int):
        with pytest.raises(ValueError, match="sample_rows"):
            run_sampled(project, sample_rows=sample_rows)


class TestTheDagOfASampleSql:
    def test_a_trouve_that_a_sample_sql_names_is_an_upstream_trouve(self, project: Path):
        """The edge holds in each run, thus --sample does not change the DAG."""
        trouves = discover_project(project)

        imports_of = {
            str(trouve.compiled.logical_address): trouve.compiled.imports
            for trouve in trouves
            if trouve.compiled is not None
        }
        assert imports_of["raw.shop.customers"] == ["ref.shop.test_customers"]

    def test_the_selection_of_a_reader_holds_the_trouve_that_the_sample_reads(
        self, project: Path
    ):
        summary, _ = run_sampled(project, select=["+*.shop.order_facts"])

        assert [str(result.addresses.logical) for result in summary.results] == [
            "ref.shop.test_customers",
            "mart.shop.order_facts",
        ]


class TestTheInputsOfADataFrameTrouve:
    def test_each_input_reads_its_sample(self, project: Path):
        _, adapter = run_sampled(project, select=[ORDER_FRAME_SELECTOR])

        sample_queries = dict(
            zip(adapter.fetched_addresses, adapter.fetched_sample_queries, strict=True)
        )
        assert sample_queries == {
            "raw.shop.orders": ORDERS_SAMPLE,
            "raw.shop.countries": COUNTRIES_SAMPLE,
        }

    def test_a_run_without_a_sample_reads_each_row_of_each_input(self, project: Path):
        _, adapter = run_sampled(project, sample=False, select=[ORDER_FRAME_SELECTOR])

        assert adapter.fetched_sample_queries == [None, None]


class TestTheTestsOfASampledRun:
    def test_the_run_skips_each_test_that_cannot_run_on_a_sample(self, project: Path):
        """TestRowCount by default, and TestUnique because it gives run_with_sample=False."""
        summary, _ = run_sampled(project)

        test_types = [
            test_result.test_type
            for test_result in summary.result("mart.shop.order_facts").test_results
        ]
        assert test_types == ["not_null"]

    def test_a_test_reads_the_complete_object_that_the_run_built(self, project: Path):
        summary, _ = run_sampled(project)

        (test_result,) = summary.result("mart.shop.order_facts").test_results
        assert "LIMIT" not in test_result.statement.sql

    def test_a_run_without_a_sample_runs_each_test(self, project: Path):
        summary, _ = run_sampled(project, sample=False)

        test_types = [
            test_result.test_type
            for test_result in summary.result("mart.shop.order_facts").test_results
        ]
        assert test_types == ["not_null", "row_count", "unique"]


class TestASampledTestRun:
    def sql_of_the_tests(self, project: Path, **options: Any) -> dict[str, str]:
        summary = clair.test(
            project, env=make_environment("dev"), adapter=RecordingAdapter(), **options
        )
        return {str(result.address): result.statement.sql for result in summary.results}

    def test_a_test_reads_the_row_limit_of_its_trouve(self, project: Path):
        sql_of_test = self.sql_of_the_tests(project, sample=True, sample_rows=5)

        assert "FROM (SELECT * FROM dev_mart.shop.order_facts LIMIT 5)" in sql_of_test[
            "dev_mart.shop.order_facts"
        ]

    def test_a_test_reads_the_sample_sql_of_its_trouve(self, project: Path):
        sql_of_test = self.sql_of_the_tests(project, sample=True)

        assert (
            "FROM (SELECT * FROM dev_mart.shop.daily_orders WHERE order_date >= '2024-07-01')"
            in sql_of_test["dev_mart.shop.daily_orders"]
        )

    def test_a_test_run_without_a_sample_reads_the_object(self, project: Path):
        sql_of_test = self.sql_of_the_tests(project)

        assert "LIMIT" not in sql_of_test["dev_mart.shop.order_facts"]


class TestASampledCompile:
    """``clair.compile(sample=True)`` writes the SQL of a sampled run, with no connection."""

    def compiled_sql_of(self, project: Path, address: str, **options: Any) -> str:
        output = clair.compile(project, env="dev", **options)
        node_info = output.node(address)
        assert node_info is not None
        return "\n".join(node_info.sql)

    def test_a_compile_with_a_sample_reads_the_sample_of_each_input(self, project: Path):
        compiled_sql = self.compiled_sql_of(
            project, "mart.shop.order_facts", sample=True, sample_rows=5
        )

        assert "FROM (SELECT * FROM raw.shop.orders LIMIT 5) AS o" in compiled_sql
        assert CUSTOMERS_SAMPLE in compiled_sql
        assert COUNTRIES_SAMPLE in compiled_sql

    def test_the_compiled_sql_is_the_sql_of_the_sampled_run(self, project: Path):
        summary, _ = run_sampled(project, select=[DAILY_ORDERS_SELECTOR], test=False)

        compiled_sql = self.compiled_sql_of(
            project,
            "mart.shop.daily_orders",
            sample=True,
            select=[DAILY_ORDERS_SELECTOR],
            # A run with test=False writes to the physical address directly.
            use_staging=False,
        )

        # The plan adds a comment line for the object type check, and the run does not send it.
        assert compiled_sql.endswith(sql_of(summary, "mart.shop.daily_orders"))
        assert f"FROM (SELECT * FROM mart.shop.order_facts LIMIT {DEFAULT_SAMPLE_ROWS})" in compiled_sql

    def test_the_output_names_the_row_limit(self, project: Path):
        assert clair.compile(project, env="dev", sample=True, sample_rows=5).sample_rows == 5
        assert clair.compile(project, env="dev").sample_rows is None

    def test_a_compile_without_a_sample_reads_each_row(self, project: Path):
        compiled_sql = self.compiled_sql_of(project, "mart.shop.order_facts")

        assert "LIMIT" not in compiled_sql

    def test_a_sampled_compile_for_production_stops(self, project: Path):
        with pytest.raises(SampleTargetsProductionError):
            clair.compile(project, env="prod", sample=True)

    @pytest.mark.parametrize("sample_rows", [0, -1])
    def test_a_row_limit_below_one_raises(self, project: Path, sample_rows: int):
        with pytest.raises(ValueError, match="sample_rows must be 1 or more"):
            clair.compile(project, env="dev", sample=True, sample_rows=sample_rows)


class TestASampleNeverReachesProduction:
    def test_an_environment_without_a_routing_entry_stops_the_run(self, project: Path):
        adapter = recording_adapter()

        with pytest.raises(SampleTargetsProductionError) as raised:
            clair.run(project, env=make_environment("prod"), adapter=adapter, sample=True)

        assert raised.value.logical_addresses == [
            "mart.shop.daily_orders",
            "mart.shop.order_facts",
            "mart.shop.order_frame",
            "ref.shop.test_customers",
        ]
        assert adapter.record.statements == []

    def test_the_guard_names_only_the_selected_trouves(self, project: Path):
        with pytest.raises(SampleTargetsProductionError) as raised:
            clair.run(
                project,
                env=make_environment("prod"),
                adapter=recording_adapter(),
                sample=True,
                select=["mart.shop.daily_orders"],
            )

        assert raised.value.logical_addresses == ["mart.shop.daily_orders"]

    def test_a_run_without_a_sample_can_write_to_production(self, project: Path):
        summary = clair.run(project, env=make_environment("prod"), adapter=recording_adapter())

        assert summary.failed_count == 0


class TestTheSampleOfATrouve:
    @pytest.mark.parametrize("sample_sql", ["", "   "])
    def test_an_empty_sample_sql_raises_at_construction(self, sample_sql: str):
        with pytest.raises(ValueError, match="sample_sql is empty"):
            Source(sample_sql=sample_sql)

    def test_a_subclass_can_change_the_sample_of_each_trouve(self):
        class TopRowsTable(Table):
            def sample(self, sample_rows: int) -> str:
                return f"(SELECT TOP {sample_rows} * FROM {clair.THIS})"

        assert TopRowsTable(sql="select 1").sample(3) == "(SELECT TOP 3 * FROM __CLAIR_THIS__)"


class TestTheRunWithSampleOfATest:
    @pytest.mark.parametrize(
        ("test", "is_run_with_sample"),
        [
            (TestNotNull(column="id"), True),
            (TestNotNull(column="id", run_with_sample=False), False),
            (clair.TestRowCount(min_rows=1), False),
            (clair.TestRowCount(min_rows=1, run_with_sample=True), True),
            (clair.TestSql(sql="select 1"), False),
        ],
    )
    def test_the_field_replaces_the_default_of_the_class(
        self, test: clair.Test, is_run_with_sample: bool
    ):
        assert test.is_run_with_sample is is_run_with_sample
