"""The tests of the discovery layer."""

from __future__ import annotations

import sys
import textwrap
from pathlib import Path

from clair.core.discovery import (
    compute_full_name,
    discover_project,
    find_routing_collisions,
    recompile_for_selection,
)
from clair.environments.routing import DatabaseOverrideRouting, SchemaIsolationRouting
from clair.trouves._refs import TROUVE_PLACEHOLDER_PREFIX
from clair.trouves.run_config import RunMode
from clair.trouves.test import TestSql
from clair.trouves.trouve import TrouveType


class TestComputeFullName:
    def test_standard_three_level(self):
        file_path = Path("/some/project/root/analytics/revenue/daily_orders.py")
        assert compute_full_name(file_path) == "analytics.revenue.daily_orders"

    def test_deep_path(self):
        file_path = Path("/a/b/c/d/database/schema/table.py")
        assert compute_full_name(file_path) == "database.schema.table"


class TestDiscovery:
    def test_discovers_simple_project(self, simple_project: Path):
        trouves = discover_project(simple_project)
        names = {t.full_name for t in trouves}

        assert "source.raw.orders" in names
        assert "analytics.revenue.daily_orders" in names
        assert len(trouves) == 2

    def test_source_trouve_has_correct_type(self, simple_project: Path):
        trouves = discover_project(simple_project)
        source = next(t for t in trouves if t.full_name == "source.raw.orders")
        assert source.type == TrouveType.SOURCE

    def test_table_trouve_has_correct_type(self, simple_project: Path):
        trouves = discover_project(simple_project)
        table = next(t for t in trouves if t.full_name == "analytics.revenue.daily_orders")
        assert table.type == TrouveType.TABLE

    def test_full_name_set_on_compiled(self, simple_project: Path):
        trouves = discover_project(simple_project)
        source = next(t for t in trouves if t.full_name == "source.raw.orders")
        assert source.full_name == "source.raw.orders"

    def test_import_detection(self, simple_project: Path):
        trouves = discover_project(simple_project)
        table = next(t for t in trouves if t.full_name == "analytics.revenue.daily_orders")
        assert table.compiled is not None
        assert "source.raw.orders" in table.compiled.imports

    def test_resolved_sql_contains_full_name(self, simple_project: Path):
        trouves = discover_project(simple_project)
        table = next(t for t in trouves if t.full_name == "analytics.revenue.daily_orders")
        assert table.compiled is not None
        assert "source.raw.orders" in table.compiled.resolved_sql

    def test_raw_sql_contains_placeholder(self, simple_project: Path):
        trouves = discover_project(simple_project)
        table = next(t for t in trouves if t.full_name == "analytics.revenue.daily_orders")
        assert TROUVE_PLACEHOLDER_PREFIX in table.sql

    def test_config_resolution(self, simple_project: Path):
        trouves = discover_project(simple_project)
        table = next(t for t in trouves if t.full_name == "analytics.revenue.daily_orders")
        assert table.compiled is not None
        assert table.compiled.config.warehouse == "reporting_wh"

    def test_skips_underscore_files(self, simple_project: Path):
        trouves = discover_project(simple_project)
        names = {t.full_name for t in trouves}
        assert not any("schema_config" in name for name in names)
        assert not any("database_config" in name for name in names)

    def test_skips_underscore_prefixed_file(self, tmp_path: Path):
        db_dir = tmp_path / "mydb" / "myschema"
        db_dir.mkdir(parents=True)
        db_dir.joinpath("real_table.py").write_text(textwrap.dedent("""\
            from clair import Trouve, TrouveType
            trouve = Trouve(type=TrouveType.SOURCE)
        """))
        db_dir.joinpath("_ignored_table.py").write_text(textwrap.dedent("""\
            from clair import Trouve, TrouveType
            trouve = Trouve(type=TrouveType.SOURCE)
        """))
        trouves = discover_project(tmp_path)
        names = {t.compiled.logical_name for t in trouves if t.compiled}
        assert "mydb.myschema.real_table" in names
        assert not any("_ignored_table" in name for name in names)

    def test_skips_underscore_prefixed_directory(self, tmp_path: Path):
        normal_dir = tmp_path / "mydb" / "myschema"
        normal_dir.mkdir(parents=True)
        normal_dir.joinpath("real_table.py").write_text(textwrap.dedent("""\
            from clair import Trouve, TrouveType
            trouve = Trouve(type=TrouveType.SOURCE)
        """))
        ignored_dir = tmp_path / "mydb" / "_ignored_schema"
        ignored_dir.mkdir(parents=True)
        ignored_dir.joinpath("also_ignored.py").write_text(textwrap.dedent("""\
            from clair import Trouve, TrouveType
            trouve = Trouve(type=TrouveType.SOURCE)
        """))
        trouves = discover_project(tmp_path)
        names = {t.compiled.logical_name for t in trouves if t.compiled}
        assert "mydb.myschema.real_table" in names
        assert not any("_ignored_schema" in name for name in names)

    def test_empty_project(self, tmp_path: Path):
        trouves = discover_project(tmp_path)
        assert len(trouves) == 0

    def test_trouves_are_compiled(self, simple_project: Path):
        trouves = discover_project(simple_project)
        assert all(t.is_compiled for t in trouves)


class TestDiscoveryWithRouting:
    """The integration tests of discover_project with a routing policy."""

    def test_database_override_remaps_table_full_name(self, simple_project: Path):
        routing = DatabaseOverrideRouting(database_name="MYDEV")
        trouves = discover_project(simple_project, routing=routing)
        table = next(t for t in trouves if t.type == TrouveType.TABLE)

        assert table.full_name == "MYDEV.revenue.daily_orders"

    def test_database_override_source_not_rerouted(self, simple_project: Path):
        routing = DatabaseOverrideRouting(database_name="MYDEV")
        trouves = discover_project(simple_project, routing=routing)
        source = next(t for t in trouves if t.type == TrouveType.SOURCE)

        assert source.full_name == "source.raw.orders"

    def test_database_override_logical_name_preserved(self, simple_project: Path):
        routing = DatabaseOverrideRouting(database_name="MYDEV")
        trouves = discover_project(simple_project, routing=routing)
        table = next(t for t in trouves if t.type == TrouveType.TABLE)

        assert table.compiled is not None
        assert table.compiled.logical_name == "analytics.revenue.daily_orders"

    def test_database_override_sql_uses_routed_names(self, simple_project: Path):
        routing = DatabaseOverrideRouting(database_name="MYDEV")
        trouves = discover_project(simple_project, routing=routing)
        table = next(t for t in trouves if t.type == TrouveType.TABLE)

        assert table.compiled is not None
        assert "source.raw.orders" in table.compiled.resolved_sql

    def test_database_override_dag_imports_use_logical_names(self, simple_project: Path):
        routing = DatabaseOverrideRouting(database_name="MYDEV")
        trouves = discover_project(simple_project, routing=routing)
        table = next(t for t in trouves if t.type == TrouveType.TABLE)

        assert table.compiled is not None
        assert "source.raw.orders" in table.compiled.imports

    def test_schema_isolation_remaps_table_full_name(self, simple_project: Path):
        routing = SchemaIsolationRouting(database_name="DEV", schema_name="myschema")
        trouves = discover_project(simple_project, routing=routing)
        table = next(t for t in trouves if t.type == TrouveType.TABLE)

        assert table.full_name == "DEV.myschema.ANALYTICS_REVENUE_DAILY_ORDERS"

    def test_no_routing_logical_and_full_name_equal(self, simple_project: Path):
        trouves = discover_project(simple_project, routing=None)

        for t in trouves:
            assert t.compiled is not None
            assert t.full_name == t.compiled.logical_name

    def test_routing_collision_continues_and_is_detectable(self, tmp_path: Path):
        source_dir = tmp_path / "shared" / "raw"
        source_dir.mkdir(parents=True)
        source_dir.joinpath("data.py").write_text(textwrap.dedent("""\
            from clair import Column, ColumnType, Trouve, TrouveType

            trouve = Trouve(
                type=TrouveType.SOURCE,
                docs="Shared source.",
                columns=[Column(name="id", type=ColumnType.STRING)],
            )
        """))

        for db_name in ("analytics", "warehouse"):
            table_dir = tmp_path / db_name / "finance"
            table_dir.mkdir(parents=True)
            table_dir.joinpath("orders.py").write_text(textwrap.dedent(f"""\
                from clair import Column, ColumnType, Trouve, TrouveType

                trouve = Trouve(
                    type=TrouveType.TABLE,
                    docs="Orders from {db_name}.",
                    sql="select 1",
                    columns=[Column(name="id", type=ColumnType.STRING)],
                )
            """))

        routing = DatabaseOverrideRouting(database_name="DEV")

        trouves = discover_project(tmp_path, routing=routing)

        collisions = find_routing_collisions(trouves)
        assert len(collisions) == 1
        target, sources = collisions[0]
        assert target == "DEV.finance.orders"
        assert len(sources) == 2


def _make_chained_project(tmp_path: Path) -> Path:
    """Make a chain of 4 Trouves, as in the example_1 scenario of the user.

    The structure is:
        mydb.source.events       [SOURCE]
        mydb.refined.events      [TABLE]  reads source.events
        mydb.derived.daily       [TABLE]  reads refined.events
        mydb.derived.top         [TABLE]  reads derived.daily
    """
    for dirs in [
        "mydb/source",
        "mydb/refined",
        "mydb/derived",
    ]:
        (tmp_path / dirs).mkdir(parents=True, exist_ok=True)

    (tmp_path / "mydb/source/events.py").write_text(textwrap.dedent("""\
        from clair import Trouve, TrouveType
        trouve = Trouve(type=TrouveType.SOURCE)
    """))

    (tmp_path / "mydb/refined/events.py").write_text(textwrap.dedent("""\
        from mydb.source.events import trouve as source_events
        from clair import Trouve, TrouveType
        trouve = Trouve(type=TrouveType.TABLE, sql=f"SELECT * FROM {source_events}")
    """))

    (tmp_path / "mydb/derived/daily.py").write_text(textwrap.dedent("""\
        from mydb.refined.events import trouve as refined_events
        from clair import Trouve, TrouveType
        trouve = Trouve(type=TrouveType.TABLE, sql=f"SELECT * FROM {refined_events}")
    """))

    (tmp_path / "mydb/derived/top.py").write_text(textwrap.dedent("""\
        from mydb.derived.daily import trouve as daily
        from clair import Trouve, TrouveType
        trouve = Trouve(type=TrouveType.TABLE, sql=f"SELECT * FROM {daily}")
    """))

    return tmp_path


class TestRecompileForSelection:
    """The tests of the SQL names in a partial run."""

    def test_before_recompile_sql_uses_logical_names(self, tmp_path: Path):
        """Before recompile_for_selection, each name in the SQL is a logical name."""
        project = _make_chained_project(tmp_path)
        routing = DatabaseOverrideRouting(database_name="omer")
        trouves = discover_project(project, routing=routing)

        daily = next(t for t in trouves if t.compiled and t.compiled.logical_name == "mydb.derived.daily")
        assert daily.compiled is not None
        # Before the selection, refined.events keeps its logical name.
        assert "mydb.refined.events" in daily.compiled.resolved_sql
        assert "omer.refined.events" not in daily.compiled.resolved_sql

    def test_partial_run_non_selected_upstream_stays_logical(self, tmp_path: Path):
        """The selector *.derived.* omits refined.events. Thus daily reads mydb.refined.events."""
        project = _make_chained_project(tmp_path)
        routing = DatabaseOverrideRouting(database_name="omer")
        trouves = discover_project(project, routing=routing)

        # This is --select '*.derived.*'. It gives derived.daily and derived.top
        # only, with the routed names that the DAG gives.
        selected = {"omer.derived.daily", "omer.derived.top"}
        recompile_for_selection(trouves, selected)

        daily = next(t for t in trouves if t.compiled and t.compiled.logical_name == "mydb.derived.daily")
        assert daily.compiled is not None
        # The selection omits refined.events, and thus it keeps its logical name.
        assert "mydb.refined.events" in daily.compiled.resolved_sql
        assert "omer.refined.events" not in daily.compiled.resolved_sql

    def test_partial_run_selected_upstream_uses_routed_name(self, tmp_path: Path):
        """top reads omer.derived.daily, because the selection contains daily."""
        project = _make_chained_project(tmp_path)
        routing = DatabaseOverrideRouting(database_name="omer")
        trouves = discover_project(project, routing=routing)

        selected = {"omer.derived.daily", "omer.derived.top"}
        recompile_for_selection(trouves, selected)

        top = next(t for t in trouves if t.compiled and t.compiled.logical_name == "mydb.derived.top")
        assert top.compiled is not None
        # The selection contains daily, and thus top reads omer.derived.daily.
        assert "omer.derived.daily" in top.compiled.resolved_sql
        assert "mydb.derived.daily" not in top.compiled.resolved_sql

    def test_full_run_all_selected_all_references_routed(self, tmp_path: Path):
        """When the selection holds each TABLE, each name between them is a routed name."""
        project = _make_chained_project(tmp_path)
        routing = DatabaseOverrideRouting(database_name="omer")
        trouves = discover_project(project, routing=routing)

        # Select each Trouve that is not a SOURCE, with the routed names.
        selected = {
            t.compiled.full_name
            for t in trouves
            if t.compiled and t.type != TrouveType.SOURCE
        }
        recompile_for_selection(trouves, selected)

        daily = next(t for t in trouves if t.compiled and t.compiled.logical_name == "mydb.derived.daily")
        assert daily.compiled is not None
        # The selection contains refined.events, and thus daily reads omer.refined.events.
        assert "omer.refined.events" in daily.compiled.resolved_sql

    def test_source_references_never_routed(self, tmp_path: Path):
        """An upstream SOURCE always keeps its logical name, whatever the selection is."""
        project = _make_chained_project(tmp_path)
        routing = DatabaseOverrideRouting(database_name="omer")
        trouves = discover_project(project, routing=routing)

        # Select each Trouve, and refined.events too, with the routed names.
        selected = {
            t.compiled.full_name
            for t in trouves
            if t.compiled and t.type != TrouveType.SOURCE
        }
        recompile_for_selection(trouves, selected)

        refined = next(t for t in trouves if t.compiled and t.compiled.logical_name == "mydb.refined.events")
        assert refined.compiled is not None
        # source.events is a SOURCE, and thus its name is always logical.
        assert "mydb.source.events" in refined.compiled.resolved_sql
        assert "omer.source.events" not in refined.compiled.resolved_sql

    def test_no_routing_recompile_is_noop(self, tmp_path: Path):
        """With no routing policy, recompile_for_selection changes nothing."""
        project = _make_chained_project(tmp_path)
        trouves = discover_project(project, routing=None)

        sql_before = {
            t.compiled.logical_name: t.compiled.resolved_sql
            for t in trouves if t.compiled
        }
        selected = {t.compiled.full_name for t in trouves if t.compiled}
        recompile_for_selection(trouves, selected)

        for t in trouves:
            if t.compiled:
                assert t.compiled.resolved_sql == sql_before[t.compiled.logical_name]

    def test_write_target_is_always_routed(self, tmp_path: Path):
        """The write target, full_name, is always routed, whatever the selection is."""
        project = _make_chained_project(tmp_path)
        routing = DatabaseOverrideRouting(database_name="omer")
        trouves = discover_project(project, routing=routing)

        # With an empty selection, full_name stays routed.
        recompile_for_selection(trouves, set())

        for t in trouves:
            if t.compiled and t.type != TrouveType.SOURCE:
                assert t.compiled.full_name.startswith("omer.")


class TestRecompileForSelectionTestSql:
    """The tests of discovery and recompile for a TestSql test."""

    def _make_project_with_test_sql(self, tmp_path: Path) -> Path:
        """A project where the TestSql of orders points to customers."""
        for dirs in ["mydb/source", "mydb/refined"]:
            (tmp_path / dirs).mkdir(parents=True, exist_ok=True)

        (tmp_path / "mydb/source/customers.py").write_text(textwrap.dedent("""\
            from clair import Trouve, TrouveType
            trouve = Trouve(type=TrouveType.SOURCE)
        """))

        # The TestSql of orders points to customers.
        (tmp_path / "mydb/refined/orders.py").write_text(textwrap.dedent("""\
            from mydb.source.customers import trouve as customers
            from clair import Trouve, TrouveType, TestSql, THIS
            trouve = Trouve(
                type=TrouveType.TABLE,
                sql="SELECT 1",
                tests=[
                    TestSql(sql=f"SELECT * FROM {THIS} t LEFT JOIN {customers} c ON t.cid = c.id WHERE c.id IS NULL"),
                ],
            )
        """))

        return tmp_path

    def test_discovery_resolves_cross_trouve_placeholder_in_test_sql(self, tmp_path: Path):
        """After discover_project, each token in TestSql.sql is a logical name."""
        from clair.trouves._refs import THIS_PLACEHOLDER
        project = self._make_project_with_test_sql(tmp_path)
        trouves = discover_project(project)

        orders = next(t for t in trouves if t.compiled and t.compiled.logical_name == "mydb.refined.orders")
        test = orders.tests[0]
        assert isinstance(test, TestSql)

        # Discovery replaces the THIS token and each token that points to a
        # different Trouve.
        assert "mydb.refined.orders" in test.sql
        assert "mydb.source.customers" in test.sql
        assert THIS_PLACEHOLDER not in test.sql

    def test_recompile_upgrades_cross_trouve_test_sql_refs(self, tmp_path: Path):
        """recompile_for_selection changes each name in TestSql.sql to a routed name."""
        project = self._make_project_with_test_sql(tmp_path)
        routing = DatabaseOverrideRouting(database_name="dev")
        trouves = discover_project(project, routing=routing)

        orders = next(t for t in trouves if t.compiled and t.compiled.logical_name == "mydb.refined.orders")
        assert orders.compiled is not None

        # The selection never holds a SOURCE. It holds orders only.
        selected = {orders.compiled.full_name}
        recompile_for_selection(trouves, selected)

        test = orders.tests[0]
        assert isinstance(test, TestSql)
        # customers is a SOURCE. Clair never routes it, and thus it keeps its
        # logical name.
        assert "mydb.source.customers" in test.sql

    def test_recompile_upgrades_table_refs_in_test_sql(self, tmp_path: Path):
        """A TestSql name that points to a selected upstream TABLE becomes a routed name."""
        for dirs in ["mydb/source", "mydb/refined", "mydb/derived"]:
            (tmp_path / dirs).mkdir(parents=True, exist_ok=True)

        (tmp_path / "mydb/source/raw.py").write_text(textwrap.dedent("""\
            from clair import Trouve, TrouveType
            trouve = Trouve(type=TrouveType.SOURCE)
        """))
        (tmp_path / "mydb/refined/base.py").write_text(textwrap.dedent("""\
            from mydb.source.raw import trouve as raw
            from clair import Trouve, TrouveType
            trouve = Trouve(type=TrouveType.TABLE, sql=f"SELECT * FROM {raw}")
        """))
        (tmp_path / "mydb/derived/top.py").write_text(textwrap.dedent("""\
            from mydb.refined.base import trouve as base
            from clair import Trouve, TrouveType, TestSql, THIS
            trouve = Trouve(
                type=TrouveType.TABLE,
                sql=f"SELECT * FROM {base}",
                tests=[
                    TestSql(sql=f"SELECT * FROM {THIS} t LEFT JOIN {base} b ON t.id = b.id WHERE b.id IS NULL"),
                ],
            )
        """))

        routing = DatabaseOverrideRouting(database_name="dev")
        trouves = discover_project(tmp_path, routing=routing)

        # Select the two TABLE Trouves.
        selected = {
            t.compiled.full_name
            for t in trouves
            if t.compiled and t.type != TrouveType.SOURCE
        }
        recompile_for_selection(trouves, selected)

        top = next(t for t in trouves if t.compiled and t.compiled.logical_name == "mydb.derived.top")
        test = top.tests[0]
        assert isinstance(test, TestSql)
        # The selection contains base, and thus the test SQL holds dev.refined.base.
        assert "dev.refined.base" in test.sql
        assert "mydb.refined.base" not in test.sql


class TestDiscoveryRunMode:
    """Clair sets clair.run_mode on the package before it loads the Trouve modules."""

    def _write_capture_module(self, tmp_path: Path) -> None:
        """Write a small Trouve file. It keeps the value of clair.run_mode at load time."""
        db_dir = tmp_path / "db" / "s"
        db_dir.mkdir(parents=True)
        (db_dir / "t.py").write_text(textwrap.dedent("""\
            import clair
            from clair import Trouve, TrouveType
            captured_run_mode = clair.run_mode
            trouve = Trouve(type=TrouveType.SOURCE)
        """))

    def test_run_mode_set_to_incremental(self, tmp_path: Path):
        self._write_capture_module(tmp_path)
        discover_project(tmp_path, run_mode=RunMode.INCREMENTAL)
        assert sys.modules["db.s.t"].captured_run_mode == RunMode.INCREMENTAL

    def test_run_mode_set_to_full_refresh(self, tmp_path: Path):
        self._write_capture_module(tmp_path)
        discover_project(tmp_path, run_mode=RunMode.FULL_REFRESH)
        assert sys.modules["db.s.t"].captured_run_mode == RunMode.FULL_REFRESH

    def test_run_mode_none_when_not_passed(self, tmp_path: Path):
        self._write_capture_module(tmp_path)
        discover_project(tmp_path)
        assert sys.modules["db.s.t"].captured_run_mode is None
