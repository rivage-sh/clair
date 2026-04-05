"""Tests for discovery of df_fn Trouve nodes."""

from __future__ import annotations

import textwrap
from pathlib import Path

from clair.core.discovery import discover_project
from clair.trouves.trouve import Trouve


def _make_pandas_project(tmp_path: Path) -> Path:
    """Build a minimal project with a SOURCE and a df_fn Trouve.

    Structure:
        mydb/source/events.py       [SOURCE]
        mydb/derived/summary.py     [df_fn Trouve] reads source.events
    """
    (tmp_path / "mydb" / "source").mkdir(parents=True)
    (tmp_path / "mydb" / "derived").mkdir(parents=True)

    (tmp_path / "mydb" / "source" / "events.py").write_text(textwrap.dedent("""\
        from clair import Trouve, TrouveType
        trouve = Trouve(type=TrouveType.SOURCE)
    """))

    (tmp_path / "mydb" / "derived" / "summary.py").write_text(textwrap.dedent("""\
        import pandas as pd
        from clair import Trouve, Column, ColumnType
        from mydb.source.events import trouve as source_events

        def summarize(events: pd.DataFrame = source_events) -> pd.DataFrame:
            return events

        trouve = Trouve(
            df_fn=summarize,
            columns=[
                Column(name="event_type", type=ColumnType.STRING),
                Column(name="event_count", type=ColumnType.NUMBER),
            ],
            docs="Summary of events.",
        )
    """))

    return tmp_path


def _make_mixed_project(tmp_path: Path) -> Path:
    """Build a project with SQL Trouve + df_fn Trouve.

    Structure:
        mydb/source/events.py       [SOURCE]
        mydb/refined/events.py      [TABLE, SQL] reads source.events
        mydb/derived/summary.py     [df_fn Trouve] reads refined.events
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
        from clair import Trouve
        from mydb.refined.events import trouve as refined_events

        def summarize(events: pd.DataFrame = refined_events) -> pd.DataFrame:
            return events

        trouve = Trouve(df_fn=summarize)
    """))

    return tmp_path


def _make_chained_pandas_project(tmp_path: Path) -> Path:
    """Build a project where a df_fn Trouve depends on another df_fn Trouve.

    Structure:
        mydb/source/events.py       [SOURCE]
        mydb/derived/step_one.py    [df_fn Trouve] reads source.events
        mydb/derived/step_two.py    [df_fn Trouve] reads derived.step_one
    """
    (tmp_path / "mydb" / "source").mkdir(parents=True)
    (tmp_path / "mydb" / "derived").mkdir(parents=True)

    (tmp_path / "mydb" / "source" / "events.py").write_text(textwrap.dedent("""\
        from clair import Trouve, TrouveType
        trouve = Trouve(type=TrouveType.SOURCE)
    """))

    (tmp_path / "mydb" / "derived" / "step_one.py").write_text(textwrap.dedent("""\
        import pandas as pd
        from clair import Trouve
        from mydb.source.events import trouve as source_events

        def transform_one(events: pd.DataFrame = source_events) -> pd.DataFrame:
            return events

        trouve = Trouve(df_fn=transform_one)
    """))

    (tmp_path / "mydb" / "derived" / "step_two.py").write_text(textwrap.dedent("""\
        import pandas as pd
        from clair import Trouve
        from mydb.derived.step_one import trouve as step_one

        def transform_two(step_one_data: pd.DataFrame = step_one) -> pd.DataFrame:
            return step_one_data

        trouve = Trouve(df_fn=transform_two)
    """))

    return tmp_path


class TestDfFnTrouveDetection:
    def test_df_fn_trouve_is_discovered(self, tmp_path: Path):
        project = _make_pandas_project(tmp_path)
        trouves = discover_project(project)
        names = {t.full_name for t in trouves}
        assert "mydb.derived.summary" in names

    def test_df_fn_trouve_is_trouve_instance(self, tmp_path: Path):
        project = _make_pandas_project(tmp_path)
        trouves = discover_project(project)
        summary = next(t for t in trouves if t.full_name == "mydb.derived.summary")
        assert isinstance(summary, Trouve)
        assert summary.df_fn is not None

    def test_df_fn_trouve_is_compiled(self, tmp_path: Path):
        project = _make_pandas_project(tmp_path)
        trouves = discover_project(project)
        summary = next(t for t in trouves if t.full_name == "mydb.derived.summary")
        assert summary.is_compiled


class TestDfFnTrouveDependencyExtraction:
    def test_imports_contain_upstream(self, tmp_path: Path):
        project = _make_pandas_project(tmp_path)
        trouves = discover_project(project)
        summary = next(t for t in trouves if t.full_name == "mydb.derived.summary")
        assert summary.compiled is not None
        assert "mydb.source.events" in summary.compiled.imports

    def test_chained_df_fn_trouve_has_correct_imports(self, tmp_path: Path):
        project = _make_chained_pandas_project(tmp_path)
        trouves = discover_project(project)
        step_two = next(t for t in trouves if t.full_name == "mydb.derived.step_two")
        assert step_two.compiled is not None
        assert "mydb.derived.step_one" in step_two.compiled.imports


class TestDfFnTrouveCompiledAttributes:
    def test_logical_name_set_correctly(self, tmp_path: Path):
        project = _make_pandas_project(tmp_path)
        trouves = discover_project(project)
        summary = next(t for t in trouves if t.full_name == "mydb.derived.summary")
        assert summary.compiled is not None
        assert summary.compiled.logical_name == "mydb.derived.summary"

    def test_full_name_set_correctly(self, tmp_path: Path):
        project = _make_pandas_project(tmp_path)
        trouves = discover_project(project)
        summary = next(t for t in trouves if t.full_name == "mydb.derived.summary")
        assert summary.compiled is not None
        assert summary.compiled.full_name == "mydb.derived.summary"

    def test_file_path_set_correctly(self, tmp_path: Path):
        project = _make_pandas_project(tmp_path)
        trouves = discover_project(project)
        summary = next(t for t in trouves if t.full_name == "mydb.derived.summary")
        assert summary.compiled is not None
        assert summary.compiled.file_path == Path("mydb/derived/summary.py")

    def test_resolved_sql_is_empty_for_df_fn_trouve(self, tmp_path: Path):
        project = _make_pandas_project(tmp_path)
        trouves = discover_project(project)
        summary = next(t for t in trouves if t.full_name == "mydb.derived.summary")
        assert summary.compiled is not None
        assert summary.compiled.resolved_sql == ""


class TestMixedDag:
    def test_all_nodes_discovered(self, tmp_path: Path):
        project = _make_mixed_project(tmp_path)
        trouves = discover_project(project)
        names = {t.full_name for t in trouves}
        assert "mydb.source.events" in names
        assert "mydb.refined.events" in names
        assert "mydb.derived.summary" in names
        assert len(trouves) == 3

    def test_sql_trouve_is_trouve_instance(self, tmp_path: Path):
        project = _make_mixed_project(tmp_path)
        trouves = discover_project(project)
        refined = next(t for t in trouves if t.full_name == "mydb.refined.events")
        assert isinstance(refined, Trouve)
        assert refined.df_fn is None

    def test_df_fn_trouve_has_df_fn_set(self, tmp_path: Path):
        project = _make_mixed_project(tmp_path)
        trouves = discover_project(project)
        summary = next(t for t in trouves if t.full_name == "mydb.derived.summary")
        assert isinstance(summary, Trouve)
        assert summary.df_fn is not None

    def test_df_fn_trouve_depends_on_sql_trouve(self, tmp_path: Path):
        project = _make_mixed_project(tmp_path)
        trouves = discover_project(project)
        summary = next(t for t in trouves if t.full_name == "mydb.derived.summary")
        assert summary.compiled is not None
        assert "mydb.refined.events" in summary.compiled.imports

    def test_sql_trouve_depends_on_source(self, tmp_path: Path):
        project = _make_mixed_project(tmp_path)
        trouves = discover_project(project)
        refined = next(t for t in trouves if t.full_name == "mydb.refined.events")
        assert refined.compiled is not None
        assert "mydb.source.events" in refined.compiled.imports
