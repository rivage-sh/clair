"""The tests of a sampled run: ``clair.run(sample=...)`` and ``clair run --sample``.

Each test drives ``clair.run()`` against a project on the disk, with a
``RecordingAdapter``. The project holds three sources: orders and customers
give an ``event_time``, and countries gives none. The routing entry of "dev"
sends each TABLE to a ``dev_`` database, and keeps each SOURCE.
"""

from __future__ import annotations

import textwrap
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

import clair
from clair.core.discovery import discover_project
from clair.core.runner import RunSummary
from clair.exceptions import CompileError, SampleTargetsProductionError
from clair.trouves.sample_window import SampleWindow
from clair.trouves.trouve import Source, Table
from tests.helpers import RecordingAdapter, make_environment

# A selector matches the physical address, and "dev" adds a prefix to the database.
ORDER_FRAME_SELECTOR = "*.shop.order_frame"

WINDOW = SampleWindow(
    start=datetime(2024, 7, 1, tzinfo=UTC), end=datetime(2024, 7, 8, tzinfo=UTC)
)
ORDERS_FILTER = WINDOW.row_filter("created_at")
ORDERS_SAMPLE = f"(SELECT * FROM raw.shop.orders WHERE {ORDERS_FILTER})"

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


def order_facts_file(orders_reference: str) -> str:
    """Give a Table that joins the three sources.

    *orders_reference* is the f-string expression of the orders reference, for
    example ``{orders} AS o``.
    """
    return f'''
from raw.shop.countries import trouve as countries
from raw.shop.customers import trouve as customers
from raw.shop.orders import trouve as orders

from clair import Table, TestNotNull, TestRowCount

trouve = Table(
    event_time="created_at",
    sql=f"""
        SELECT o.order_id, o.created_at, c.customer_name, k.country_name
        FROM {orders_reference}
        JOIN {{customers.unsampled()}} AS c ON o.customer_id = c.customer_id
        JOIN {{countries}} AS k ON c.country_code = k.country_code
    """,
    tests=[TestNotNull(column="order_id"), TestRowCount(min_rows=1)],
)
'''


DAILY_ORDERS_FILE = '''
from mart.shop.order_facts import trouve as order_facts

from clair import Table

trouve = Table(
    sql=f"SELECT DATE(created_at) AS order_date, COUNT(*) AS order_count FROM {order_facts} GROUP BY 1",
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
        "raw/shop/orders.py": 'from clair import Source\n\ntrouve = Source(event_time="created_at")\n',
        "raw/shop/customers.py": 'from clair import Source\n\ntrouve = Source(event_time="signed_up_at")\n',
        "raw/shop/countries.py": "from clair import Source\n\ntrouve = Source()\n",
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


def sql_of(summary: RunSummary, address: str) -> str:
    """Give the build SQL of one Trouve: each statement, joined."""
    return "\n".join(statement.sql for statement in summary.result(address).statements)


def recording_adapter() -> RecordingAdapter:
    """Give an adapter that answers each DataFrame read with one row."""
    return RecordingAdapter(
        default_dataframe=pd.DataFrame({"order_id": [1], "country_code": ["FR"]})
    )


def run_sampled(
    project: Path, sample: SampleWindow | None = WINDOW, **options: Any
) -> tuple[RunSummary, RecordingAdapter]:
    adapter = recording_adapter()
    summary = clair.run(project, env=make_environment("dev"), adapter=adapter, sample=sample, **options)
    return summary, adapter


class TestASampledReference:
    def test_a_reference_to_a_trouve_with_an_event_time_reads_the_window(self, project: Path):
        summary, _ = run_sampled(project)

        assert ORDERS_SAMPLE in sql_of(summary, "mart.shop.order_facts")

    def test_a_reference_to_a_trouve_without_an_event_time_reads_each_row(self, project: Path):
        summary, _ = run_sampled(project)

        build_sql = sql_of(summary, "mart.shop.order_facts")
        assert "JOIN raw.shop.countries AS k" in build_sql

    def test_an_unsampled_reference_reads_each_row(self, project: Path):
        summary, _ = run_sampled(project)

        build_sql = sql_of(summary, "mart.shop.order_facts")
        assert "JOIN raw.shop.customers AS c" in build_sql
        assert "signed_up_at" not in build_sql

    def test_a_reference_to_a_trouve_of_the_run_reads_the_window_of_its_physical_address(
        self, project: Path
    ):
        summary, _ = run_sampled(project)

        order_facts_address = str(summary.result("mart.shop.order_facts").addresses.physical)
        assert (
            f"(SELECT * FROM {order_facts_address} WHERE {ORDERS_FILTER})"
            in sql_of(summary, "mart.shop.daily_orders")
        )

    def test_an_unsampled_reference_makes_a_dag_edge(self, project: Path):
        trouves = discover_project(project)

        imports_of = {
            str(trouve.compiled.logical_address): trouve.compiled.imports
            for trouve in trouves
            if trouve.compiled is not None
        }
        assert sorted(imports_of["mart.shop.order_facts"]) == [
            "raw.shop.countries",
            "raw.shop.customers",
            "raw.shop.orders",
        ]

    def test_a_run_without_a_sample_reads_each_row(self, project: Path):
        summary, _ = run_sampled(project, sample=None)

        build_sql = sql_of(summary, "mart.shop.order_facts")
        assert "FROM raw.shop.orders AS o" in build_sql
        assert "created_at >=" not in build_sql
        assert summary.sample is None

    def test_the_summary_holds_the_window(self, project: Path):
        summary, _ = run_sampled(project)

        assert summary.sample == WINDOW

    def test_a_column_that_a_sampled_reference_qualifies_stops_the_run(self, tmp_path: Path):
        project = write_project(tmp_path / "shop_project", orders_reference="{orders} AS o, {orders}.order_id")

        with pytest.raises(CompileError, match="alias"):
            run_sampled(project)

    def test_a_qualified_unsampled_reference_is_permitted(self, tmp_path: Path):
        project = write_project(
            tmp_path / "shop_project",
            orders_reference="{orders.unsampled()} AS o, {orders.unsampled()}.order_id",
        )

        summary, _ = run_sampled(project)

        assert "raw.shop.orders.order_id" in sql_of(summary, "mart.shop.order_facts")


class TestTheInputsOfADataFrameTrouve:
    def test_an_input_with_an_event_time_reads_the_window(self, project: Path):
        _, adapter = run_sampled(project, select=[ORDER_FRAME_SELECTOR])

        filters = dict(zip(adapter.fetched_addresses, adapter.fetched_row_filters, strict=True))
        assert filters == {"raw.shop.orders": ORDERS_FILTER, "raw.shop.countries": None}

    def test_a_run_without_a_sample_reads_each_row_of_each_input(self, project: Path):
        _, adapter = run_sampled(project, sample=None, select=[ORDER_FRAME_SELECTOR])

        assert adapter.fetched_row_filters == [None, None]


class TestTheTestsOfASampledRun:
    def test_the_run_skips_each_test_that_needs_the_complete_data(self, project: Path):
        summary, _ = run_sampled(project)

        test_types = [
            test_result.test_type
            for test_result in summary.result("mart.shop.order_facts").test_results
        ]
        assert test_types == ["not_null"]

    def test_a_test_reads_the_complete_object_that_the_run_built(self, project: Path):
        summary, _ = run_sampled(project)

        (test_result,) = summary.result("mart.shop.order_facts").test_results
        assert "TOP 1000" not in test_result.statement.sql


class TestASampleNeverReachesProduction:
    def test_an_environment_without_a_routing_entry_stops_the_run(self, project: Path):
        adapter = RecordingAdapter()

        with pytest.raises(SampleTargetsProductionError) as raised:
            clair.run(project, env=make_environment("prod"), adapter=adapter, sample=WINDOW)

        assert raised.value.logical_addresses == [
            "mart.shop.daily_orders",
            "mart.shop.order_facts",
            "mart.shop.order_frame",
        ]
        assert adapter.record.statements == []

    def test_the_guard_names_only_the_selected_trouves(self, project: Path):
        with pytest.raises(SampleTargetsProductionError) as raised:
            clair.run(
                project,
                env=make_environment("prod"),
                adapter=RecordingAdapter(),
                sample=WINDOW,
                select=["mart.shop.daily_orders"],
            )

        assert raised.value.logical_addresses == ["mart.shop.daily_orders"]

    def test_a_run_without_a_sample_can_write_to_production(self, project: Path):
        summary = clair.run(project, env=make_environment("prod"), adapter=recording_adapter())

        assert summary.failed_count == 0


class TestTheEventTimeOfATrouve:
    def test_an_event_time_that_the_declared_columns_hold_is_permitted(self):
        trouve = Source(event_time="CREATED_AT", columns=[clair.Column(name="created_at", type="TIMESTAMP_NTZ")])

        assert trouve.event_time == "CREATED_AT"

    def test_an_event_time_without_declared_columns_is_permitted(self):
        assert Table(sql="select 1", event_time="created_at").event_time == "created_at"

    @pytest.mark.parametrize(
        ("event_time", "message"),
        [("", "empty"), ("   ", "empty"), ("updated_at", "do not hold it")],
    )
    def test_a_bad_event_time_raises_at_construction(self, event_time: str, message: str):
        with pytest.raises(ValueError, match=message):
            Source(event_time=event_time, columns=[clair.Column(name="created_at", type="DATE")])


NOW = datetime(2024, 7, 8, 12, 0, tzinfo=UTC)


class TestTheSampleWindow:
    @pytest.mark.parametrize(
        ("text", "expected_start"),
        [
            ("3 days", datetime(2024, 7, 5, 12, 0, tzinfo=UTC)),
            ("1 day", datetime(2024, 7, 7, 12, 0, tzinfo=UTC)),
            ("6 hours", datetime(2024, 7, 8, 6, 0, tzinfo=UTC)),
            ("2 WEEKS", datetime(2024, 6, 24, 12, 0, tzinfo=UTC)),
            (" 1 week ", datetime(2024, 7, 1, 12, 0, tzinfo=UTC)),
        ],
    )
    def test_a_duration_ends_now(self, text: str, expected_start: datetime):
        assert SampleWindow.parse(text, now=NOW) == SampleWindow(start=expected_start, end=NOW)

    @pytest.mark.parametrize(
        ("text", "expected"),
        [
            (
                "2024-07-01/2024-07-08",
                SampleWindow(start=datetime(2024, 7, 1, tzinfo=UTC), end=datetime(2024, 7, 8, tzinfo=UTC)),
            ),
            (
                "2024-07-01T06:00/2024-07-01T18:00",
                SampleWindow(
                    start=datetime(2024, 7, 1, 6, tzinfo=UTC), end=datetime(2024, 7, 1, 18, tzinfo=UTC)
                ),
            ),
            (
                "2024-07-01T02:00+02:00/2024-07-02",
                SampleWindow(start=datetime(2024, 7, 1, tzinfo=UTC), end=datetime(2024, 7, 2, tzinfo=UTC)),
            ),
        ],
    )
    def test_a_fixed_window_is_utc_when_it_gives_no_time_zone(self, text: str, expected: SampleWindow):
        assert SampleWindow.parse(text) == expected

    @pytest.mark.parametrize(
        "text",
        ["", "3", "days", "3 fortnights", "-3 days", "2024-07-01", "2024-07-08/2024-07-01", "2024-07-01/July"],
    )
    def test_a_text_that_has_neither_form_raises(self, text: str):
        with pytest.raises(ValueError):
            SampleWindow.parse(text)

    def test_the_start_must_be_before_the_end(self):
        with pytest.raises(ValueError, match="before the end"):
            SampleWindow(start=NOW, end=NOW)

    def test_a_moment_without_a_time_zone_raises(self):
        with pytest.raises(ValueError):
            SampleWindow(start=datetime(2024, 7, 1), end=datetime(2024, 7, 8))

    def test_the_filter_keeps_the_start_and_not_the_end(self):
        assert WINDOW.row_filter("created_at") == (
            "created_at >= '2024-07-01 00:00:00.000000'::TIMESTAMP_NTZ "
            "AND created_at < '2024-07-08 00:00:00.000000'::TIMESTAMP_NTZ"
        )

    def test_the_filter_gives_each_boundary_in_utc(self):
        window = SampleWindow.parse("2024-07-01T02:00+02:00/2024-07-01T04:00+02:00")

        assert "'2024-07-01 00:00:00.000000'" in window.row_filter("created_at")
        assert "'2024-07-01 02:00:00.000000'" in window.row_filter("created_at")
