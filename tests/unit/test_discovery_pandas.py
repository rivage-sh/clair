"""The tests of the discovery of a PandasTrouve node."""

from __future__ import annotations

import textwrap
from pathlib import Path

from clair.core.discovery import discover_project, recompile_for_selection
from clair.trouves.pandas_trouve import PandasTrouve
from clair.trouves.trouve import Trouve, TrouveAbc
from tests.helpers import DatabaseOverrideRouting


def _make_pandas_project(tmp_path: Path) -> Path:
    """Make a small project with a SOURCE and a PandasTrouve.

    The structure is:
        mydb/source/events.py       [SOURCE]
        mydb/derived/summary.py     [a PandasTrouve] reads source.events
    """
    (tmp_path / "mydb" / "source").mkdir(parents=True)
    (tmp_path / "mydb" / "derived").mkdir(parents=True)

    (tmp_path / "mydb" / "source" / "events.py").write_text(textwrap.dedent("""\
        from clair import Trouve, TrouveType
        trouve = Trouve(type=TrouveType.SOURCE)
    """))

    (tmp_path / "mydb" / "derived" / "summary.py").write_text(textwrap.dedent("""\
        import pandas as pd
        from clair import PandasTrouve, Column, ColumnType
        from mydb.source.events import trouve as source_events

        def summarize(events: pd.DataFrame) -> pd.DataFrame:
            return events

        trouve = PandasTrouve(
            transform=summarize,
            inputs=[source_events],
            columns=[
                Column(name="event_type", type=ColumnType.STRING),
                Column(name="event_count", type=ColumnType.NUMBER),
            ],
            docs="Summary of events.",
        )
    """))

    return tmp_path


def _make_mixed_project(tmp_path: Path) -> Path:
    """Make a project with a SQL Trouve and a PandasTrouve.

    The structure is:
        mydb/source/events.py       [SOURCE]
        mydb/refined/events.py      [a TABLE with SQL] reads source.events
        mydb/derived/summary.py     [a PandasTrouve] reads refined.events
    """
    (tmp_path / "mydb" / "source").mkdir(parents=True)
    (tmp_path / "mydb" / "refined").mkdir(parents=True)
    (tmp_path / "mydb" / "derived").mkdir(parents=True)

    (tmp_path / "mydb" / "source" / "events.py").write_text(textwrap.dedent("""\
        from clair import Trouve, TrouveType
        trouve = Trouve(type=TrouveType.SOURCE)
    """))

    (tmp_path / "mydb" / "refined" / "events.py").write_text(textwrap.dedent("""\
        from mydb.source.events import trouve as source_events
        from clair import Trouve, TrouveType
        trouve = Trouve(type=TrouveType.TABLE, sql=f"SELECT * FROM {source_events}")
    """))

    (tmp_path / "mydb" / "derived" / "summary.py").write_text(textwrap.dedent("""\
        import pandas as pd
        from clair import PandasTrouve
        from mydb.refined.events import trouve as refined_events

        def summarize(events: pd.DataFrame) -> pd.DataFrame:
            return events

        trouve = PandasTrouve(transform=summarize, inputs=[refined_events])
    """))

    return tmp_path


def _make_chained_pandas_project(tmp_path: Path) -> Path:
    """Make a project where one PandasTrouve depends on a different PandasTrouve.

    The structure is:
        mydb/source/events.py       [SOURCE]
        mydb/derived/step_one.py    [a PandasTrouve] reads source.events
        mydb/derived/step_two.py    [a PandasTrouve] reads derived.step_one
    """
    (tmp_path / "mydb" / "source").mkdir(parents=True)
    (tmp_path / "mydb" / "derived").mkdir(parents=True)

    (tmp_path / "mydb" / "source" / "events.py").write_text(textwrap.dedent("""\
        from clair import Trouve, TrouveType
        trouve = Trouve(type=TrouveType.SOURCE)
    """))

    (tmp_path / "mydb" / "derived" / "step_one.py").write_text(textwrap.dedent("""\
        import pandas as pd
        from clair import PandasTrouve
        from mydb.source.events import trouve as source_events

        def transform_one(events: pd.DataFrame) -> pd.DataFrame:
            return events

        trouve = PandasTrouve(transform=transform_one, inputs=[source_events])
    """))

    (tmp_path / "mydb" / "derived" / "step_two.py").write_text(textwrap.dedent("""\
        import pandas as pd
        from clair import PandasTrouve
        from mydb.derived.step_one import trouve as step_one

        def transform_two(step_one_data: pd.DataFrame) -> pd.DataFrame:
            return step_one_data

        trouve = PandasTrouve(transform=transform_two, inputs=[step_one])
    """))

    return tmp_path


class TestPandasTrouveDetection:
    def test_pandas_trouve_is_discovered(self, tmp_path: Path):
        project = _make_pandas_project(tmp_path)
        trouves = discover_project(project)
        names = {str(t.physical_address) for t in trouves}
        assert "mydb.derived.summary" in names

    def test_pandas_trouve_is_a_pandas_trouve_instance(self, tmp_path: Path):
        project = _make_pandas_project(tmp_path)
        trouves = discover_project(project)
        summary = next(t for t in trouves if str(t.physical_address) == "mydb.derived.summary")
        assert isinstance(summary, PandasTrouve)
        assert summary.transform is not None

    def test_pandas_trouve_is_compiled(self, tmp_path: Path):
        project = _make_pandas_project(tmp_path)
        trouves = discover_project(project)
        summary = next(t for t in trouves if str(t.physical_address) == "mydb.derived.summary")
        assert summary.is_compiled


class TestPandasTrouveDependencyExtraction:
    def test_imports_contain_upstream(self, tmp_path: Path):
        project = _make_pandas_project(tmp_path)
        trouves = discover_project(project)
        summary = next(t for t in trouves if str(t.physical_address) == "mydb.derived.summary")
        assert summary.compiled is not None
        assert "mydb.source.events" in summary.compiled.imports

    def test_chained_pandas_trouve_has_correct_imports(self, tmp_path: Path):
        project = _make_chained_pandas_project(tmp_path)
        trouves = discover_project(project)
        step_two = next(t for t in trouves if str(t.physical_address) == "mydb.derived.step_two")
        assert step_two.compiled is not None
        assert "mydb.derived.step_one" in step_two.compiled.imports


class TestPandasTrouveCompiledAttributes:
    def test_logical_address_set_correctly(self, tmp_path: Path):
        project = _make_pandas_project(tmp_path)
        trouves = discover_project(project)
        summary = next(t for t in trouves if str(t.physical_address) == "mydb.derived.summary")
        assert summary.compiled is not None
        assert str(summary.compiled.logical_address) == "mydb.derived.summary"

    def test_address_set_correctly(self, tmp_path: Path):
        project = _make_pandas_project(tmp_path)
        trouves = discover_project(project)
        summary = next(t for t in trouves if str(t.physical_address) == "mydb.derived.summary")
        assert summary.compiled is not None
        assert str(summary.compiled.physical_address) == "mydb.derived.summary"

    def test_file_path_set_correctly(self, tmp_path: Path):
        project = _make_pandas_project(tmp_path)
        trouves = discover_project(project)
        summary = next(t for t in trouves if str(t.physical_address) == "mydb.derived.summary")
        assert summary.compiled is not None
        assert summary.compiled.file_path == Path("mydb/derived/summary.py")

    def test_resolved_sql_is_empty_for_pandas_trouve(self, tmp_path: Path):
        project = _make_pandas_project(tmp_path)
        trouves = discover_project(project)
        summary = next(t for t in trouves if str(t.physical_address) == "mydb.derived.summary")
        assert summary.compiled is not None
        assert summary.compiled.resolved_sql == ""


class TestMixedDag:
    def test_all_nodes_discovered(self, tmp_path: Path):
        project = _make_mixed_project(tmp_path)
        trouves = discover_project(project)
        names = {str(t.physical_address) for t in trouves}
        assert "mydb.source.events" in names
        assert "mydb.refined.events" in names
        assert "mydb.derived.summary" in names
        assert len(trouves) == 3

    def test_sql_trouve_is_trouve_instance(self, tmp_path: Path):
        project = _make_mixed_project(tmp_path)
        trouves = discover_project(project)
        refined = next(t for t in trouves if str(t.physical_address) == "mydb.refined.events")
        assert isinstance(refined, Trouve)
        assert not isinstance(refined, PandasTrouve)

    def test_pandas_trouve_has_a_transform(self, tmp_path: Path):
        project = _make_mixed_project(tmp_path)
        trouves = discover_project(project)
        summary = next(t for t in trouves if str(t.physical_address) == "mydb.derived.summary")
        assert isinstance(summary, PandasTrouve)
        assert summary.transform is not None

    def test_pandas_trouve_depends_on_sql_trouve(self, tmp_path: Path):
        project = _make_mixed_project(tmp_path)
        trouves = discover_project(project)
        summary = next(t for t in trouves if str(t.physical_address) == "mydb.derived.summary")
        assert summary.compiled is not None
        assert "mydb.refined.events" in summary.compiled.imports

    def test_sql_trouve_depends_on_source(self, tmp_path: Path):
        project = _make_mixed_project(tmp_path)
        trouves = discover_project(project)
        refined = next(t for t in trouves if str(t.physical_address) == "mydb.refined.events")
        assert refined.compiled is not None
        assert "mydb.source.events" in refined.compiled.imports


class TestPandasInputAddresses:
    """The tests of input_addresses, the read address of each pandas input.

    A pandas Trouve names its inputs in a list, and not in SQL. These tests
    confirm that the list obeys the same rule as the SQL of a SQL Trouve.
    """

    def _addresses_of(
        self, trouves: list[TrouveAbc], logical_address: str
    ) -> list[str]:
        trouve = next(
            t for t in trouves
            if t.compiled and str(t.compiled.logical_address) == logical_address
        )
        assert trouve.compiled is not None
        return trouve.compiled.input_addresses

    def test_discovery_gives_the_logical_address(self, tmp_path: Path):
        """Before the selection, each input holds its logical address."""
        project = _make_chained_pandas_project(tmp_path)
        trouves = discover_project(
            project, routing=DatabaseOverrideRouting(database_name="omer")
        )

        assert self._addresses_of(trouves, "mydb.derived.step_two") == [
            "mydb.derived.step_one"
        ]

    def test_a_selected_input_takes_its_physical_address(self, tmp_path: Path):
        """This run builds step_one, thus step_two reads the new table."""
        project = _make_chained_pandas_project(tmp_path)
        trouves = discover_project(
            project, routing=DatabaseOverrideRouting(database_name="omer")
        )

        recompile_for_selection(
            trouves, {"omer.derived.step_one", "omer.derived.step_two"}
        )

        assert self._addresses_of(trouves, "mydb.derived.step_two") == [
            "omer.derived.step_one"
        ]

    def test_an_input_outside_the_selection_stays_logical(self, tmp_path: Path):
        """This run does not build step_one, thus step_two reads production."""
        project = _make_chained_pandas_project(tmp_path)
        trouves = discover_project(
            project, routing=DatabaseOverrideRouting(database_name="omer")
        )

        recompile_for_selection(trouves, {"omer.derived.step_two"})

        assert self._addresses_of(trouves, "mydb.derived.step_two") == [
            "mydb.derived.step_one"
        ]

    def test_a_routed_source_input_takes_its_physical_address(self, tmp_path: Path):
        """Clair never builds a SOURCE, thus the routing entry decides its address."""
        project = _make_chained_pandas_project(tmp_path)
        trouves = discover_project(
            project, routing=DatabaseOverrideRouting(database_name="omer")
        )

        recompile_for_selection(trouves, {"omer.derived.step_one"})

        assert self._addresses_of(trouves, "mydb.derived.step_one") == [
            "omer.source.events"
        ]

    def test_no_routing_keeps_each_input_logical(self, tmp_path: Path):
        """With no routing entry, the logical address is the physical address."""
        project = _make_chained_pandas_project(tmp_path)
        trouves = discover_project(project, routing=None)

        recompile_for_selection(
            trouves, {"mydb.derived.step_one", "mydb.derived.step_two"}
        )

        assert self._addresses_of(trouves, "mydb.derived.step_two") == [
            "mydb.derived.step_one"
        ]
        assert self._addresses_of(trouves, "mydb.derived.step_one") == [
            "mydb.source.events"
        ]
