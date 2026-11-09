"""A notebook imports a Trouve, gives its address, and reads it into pandas.

No discovery runs before the import. The file of the Trouve gives the logical
address, and ``__routing__.py`` gives the physical address of each environment.
"""

from __future__ import annotations

import importlib
from pathlib import Path

import pandas as pd
import pytest
import snowflake.connector
from structlog.testing import capture_logs

import clair
from clair import SnowflakeAdapter, Table
from clair.exceptions import ProjectRootNotFoundError, TrouveFileNotFoundError
from clair.trouves.address import TrouveAddress
from clair.trouves.trouve import TrouveAbc
from tests.helpers import RecordingAdapter, make_environment

ROUTING_FILE = """
from clair import RoutingEntry, RoutingTable, TrouveAddress, TrouveType


class DeveloperRouting(RoutingEntry):
    environment_name: str = "dev"

    def route(self, trouve_address: TrouveAddress, trouve_type: TrouveType) -> TrouveAddress:
        if trouve_type == TrouveType.SOURCE:
            return trouve_address
        return trouve_address.model_copy(
            update={"database_name": f"{trouve_address.database_name}_dev"}
        )


class ProductionRouting(RoutingEntry):
    environment_name: str = "prod"

    def route(self, trouve_address: TrouveAddress, trouve_type: TrouveType) -> TrouveAddress:
        return trouve_address


routing = RoutingTable(entries=[DeveloperRouting(), ProductionRouting()])
"""

SOURCE_FILE = """
from clair import Source

trouve = Source()
"""

TABLE_FILE = """
from analytics.source.events import trouve as events
from clair import Table

trouve = Table(sql=f"select * from {events}")
"""


@pytest.fixture
def project_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Write a project with a SOURCE and a TABLE, and put its root on sys.path."""
    root = tmp_path / "project"
    for relative_path, text in [
        ("__routing__.py", ROUTING_FILE),
        ("analytics/source/events.py", SOURCE_FILE),
        ("analytics/derived/orders.py", TABLE_FILE),
    ]:
        file_path = root / relative_path
        file_path.parent.mkdir(parents=True, exist_ok=True)
        file_path.write_text(text)
    monkeypatch.syspath_prepend(str(root))
    return root


def import_trouve(module_name: str) -> TrouveAbc:
    """Import a Trouve as a notebook does: a plain import, with no discovery."""
    return importlib.import_module(module_name).trouve


class TestTheAddressOfAnImportedTrouve:
    def test_the_logical_address_needs_no_discovery(self, project_root: Path):
        orders = import_trouve("analytics.derived.orders")

        assert orders.compiled is None
        assert str(orders.logical_address) == "analytics.derived.orders"

    @pytest.mark.parametrize(
        ("module_name", "env_name", "expected_address"),
        [
            ("analytics.derived.orders", "dev", "analytics_dev.derived.orders"),
            ("analytics.derived.orders", "prod", "analytics.derived.orders"),
            ("analytics.source.events", "dev", "analytics.source.events"),
            ("analytics.source.events", "prod", "analytics.source.events"),
        ],
    )
    def test_the_routing_entry_of_the_environment_gives_the_physical_address(
        self, project_root: Path, module_name: str, env_name: str, expected_address: str
    ):
        trouve = import_trouve(module_name)
        assert str(trouve.get_physical_address(env_name)) == expected_address

    @pytest.mark.parametrize("env_name", ["dev", "prod"])
    def test_a_run_writes_to_the_same_address(self, project_root: Path, env_name: str):
        """The method and clair.compile() route through one function."""
        output = clair.compile(project_root, env=env_name)
        orders = import_trouve("analytics.derived.orders")

        node = output.node("analytics.derived.orders")
        assert node is not None
        assert orders.get_physical_address(env_name) == node.addresses.physical

    def test_an_import_before_a_compile_keeps_its_address(self, project_root: Path):
        """clair.compile() runs each file again, and the old object keeps its file."""
        orders = import_trouve("analytics.derived.orders")
        assert str(orders.logical_address) == "analytics.derived.orders"

        clair.compile(project_root, env="dev")

        assert str(orders.get_physical_address("dev")) == "analytics_dev.derived.orders"

    def test_an_environment_without_an_entry_gives_passthrough_and_a_warning(
        self, project_root: Path
    ):
        orders = import_trouve("analytics.derived.orders")

        with capture_logs() as log_entries:
            physical_address = orders.get_physical_address("staging")

        assert physical_address == orders.logical_address
        assert [entry["event"] for entry in log_entries] == ["routing.unnamed_environment"]

    def test_a_file_outside_a_project_raises(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ):
        loose_root = tmp_path.parent / f"{tmp_path.name}_loose"
        file_path = loose_root / "analytics" / "source" / "events.py"
        file_path.parent.mkdir(parents=True)
        file_path.write_text(SOURCE_FILE)
        monkeypatch.syspath_prepend(str(loose_root))

        events = import_trouve("analytics.source.events")

        assert str(events.logical_address) == "analytics.source.events"
        with pytest.raises(ProjectRootNotFoundError):
            events.get_physical_address("dev")

    def test_a_trouve_without_a_file_has_no_physical_address(self):
        with pytest.raises(TrouveFileNotFoundError):
            Table(sql="select 1 as n").get_physical_address("dev")


class TestFetchADataframe:
    def test_a_trouve_reads_its_production_address(self, project_root: Path):
        orders_rows = pd.DataFrame({"order_id": [1, 2]})
        adapter = RecordingAdapter(dataframes={"analytics.derived.orders": orders_rows})

        dataframe = adapter.fetch_dataframe(import_trouve("analytics.derived.orders"))

        assert dataframe is orders_rows
        assert adapter.fetched_addresses == ["analytics.derived.orders"]

    def test_the_physical_address_of_an_environment_reads_that_environment(
        self, project_root: Path
    ):
        orders = import_trouve("analytics.derived.orders")
        adapter = RecordingAdapter(default_dataframe=pd.DataFrame())

        adapter.fetch_dataframe(orders.get_physical_address("dev"))

        assert adapter.fetched_addresses == ["analytics_dev.derived.orders"]

    def test_an_address_reads_that_address(self):
        adapter = RecordingAdapter(default_dataframe=pd.DataFrame())

        adapter.fetch_dataframe(TrouveAddress.parse("db.s.t"), sample_query="(select 1)")

        assert adapter.fetched_addresses == ["db.s.t"]
        assert adapter.fetched_sample_queries == ["(select 1)"]

    def test_a_trouve_without_a_file_raises(self):
        adapter = RecordingAdapter(default_dataframe=pd.DataFrame())

        with pytest.raises(TrouveFileNotFoundError):
            adapter.fetch_dataframe(Table(sql="select 1 as n"))
        assert adapter.fetched_addresses == []


class TestTheConnectedAdapter:
    def test_one_call_makes_and_connects_the_adapter(self, monkeypatch: pytest.MonkeyPatch):
        connect_arguments: list[dict[str, object]] = []

        def fake_connect(**arguments: object) -> object:
            connect_arguments.append(arguments)
            return object()

        monkeypatch.setattr(snowflake.connector, "connect", fake_connect)

        adapter = SnowflakeAdapter.connected(make_environment(account="ab12345"))
        adapter.new_connection()

        assert [arguments["account"] for arguments in connect_arguments] == ["ab12345", "ab12345"]
