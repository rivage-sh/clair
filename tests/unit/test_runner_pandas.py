"""The tests of _run_pandas_trouve, the pandas path of the runner."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd

from clair.adapters.base import QueryResult, WarehouseAdapter
from clair.core.runner import RunStatus, _run_pandas_trouve
from clair.trouves.config import ResolvedConfig
from clair.trouves.pandas_trouve import PandasTrouve
from clair.trouves.trouve import CompiledAttributes, ExecutionType, Trouve, TrouveType


def _make_compiled(full_name: str = "db.schema.table") -> CompiledAttributes:
    return CompiledAttributes(
        full_name=full_name,
        logical_name=full_name,
        resolved_sql="",
        file_path=Path(f"/fake/{full_name.replace('.', '/')}.py"),
        module_name=full_name,
        imports=[],
        config=ResolvedConfig(),
        execution_type=ExecutionType.PANDAS,
    )


def _make_source(full_name: str = "db.schema.source") -> Trouve:
    source = Trouve(type=TrouveType.SOURCE)
    source.compiled = _make_compiled(full_name)
    return source


def _make_df_adapter(
    fetch_dataframes: dict[str, pd.DataFrame] | None = None,
    write_success: bool = True,
    fetch_side_effect: Exception | None = None,
    write_side_effect: Exception | None = None,
) -> MagicMock:
    """Make a false adapter that obeys the DataFrameCapableAdapter protocol."""
    adapter = MagicMock(spec=WarehouseAdapter)

    # Add the DataFrame methods. Thus isinstance() accepts the Protocol.
    adapter.fetch_dataframe = MagicMock()
    adapter.write_dataframe = MagicMock()

    if fetch_side_effect:
        adapter.fetch_dataframe.side_effect = fetch_side_effect
    elif fetch_dataframes is not None:
        def _fetch(full_name: str) -> pd.DataFrame:
            return fetch_dataframes[full_name]
        adapter.fetch_dataframe.side_effect = _fetch
    else:
        adapter.fetch_dataframe.return_value = pd.DataFrame({"col": [1, 2, 3]})

    if write_side_effect:
        adapter.write_dataframe.side_effect = write_side_effect
    else:
        adapter.write_dataframe.return_value = QueryResult(
            query_id="write-qid-001",
            query_url="https://test/#/query/write-qid-001",
            success=write_success,
        )

    return adapter


class TestRunPandasTrouveHappyPath:
    def test_transform_called_and_result_written(self):
        source = _make_source("db.schema.events")
        input_df = pd.DataFrame({"event_type": ["a", "b"], "count": [1, 2]})
        result_df = pd.DataFrame({"summary": [3]})

        def my_fn(events: pd.DataFrame) -> pd.DataFrame:
            return result_df

        trouve = PandasTrouve(transform=my_fn, inputs=[source])
        trouve.compiled = _make_compiled("db.schema.summary")

        adapter = _make_df_adapter(fetch_dataframes={"db.schema.events": input_df})

        result = _run_pandas_trouve(trouve, adapter)

        assert result.status == RunStatus.SUCCESS
        assert result.full_name == "db.schema.summary"
        adapter.write_dataframe.assert_called_once()
        call_kwargs = adapter.write_dataframe.call_args
        pd.testing.assert_frame_equal(call_kwargs.kwargs["dataframe"], result_df)

    def test_fetch_called_for_each_input(self):
        source_a = _make_source("db.schema.a")
        source_b = _make_source("db.schema.b")
        df_a = pd.DataFrame({"x": [1]})
        df_b = pd.DataFrame({"y": [2]})

        def my_fn(a: pd.DataFrame, b: pd.DataFrame) -> pd.DataFrame:
            return pd.DataFrame({"z": [3]})

        trouve = PandasTrouve(transform=my_fn, inputs=[source_a, source_b])
        trouve.compiled = _make_compiled("db.schema.output")

        adapter = _make_df_adapter(
            fetch_dataframes={"db.schema.a": df_a, "db.schema.b": df_b}
        )

        result = _run_pandas_trouve(trouve, adapter)

        assert result.status == RunStatus.SUCCESS
        assert adapter.fetch_dataframe.call_count == 2

    def test_transform_receives_the_fetched_dataframe(self):
        source = _make_source("db.schema.events")
        input_df = pd.DataFrame({"col": [1]})
        received_kwargs = {}

        def my_fn(events: pd.DataFrame) -> pd.DataFrame:
            received_kwargs["events"] = events
            return pd.DataFrame({"out": [1]})

        trouve = PandasTrouve(transform=my_fn, inputs=[source])
        trouve.compiled = _make_compiled("db.schema.summary")

        adapter = _make_df_adapter(fetch_dataframes={"db.schema.events": input_df})
        _run_pandas_trouve(trouve, adapter)

        assert "events" in received_kwargs
        pd.testing.assert_frame_equal(received_kwargs["events"], input_df)


class TestRunPandasTrouveFullNameParsing:
    def test_database_schema_table_parsed_correctly(self):
        source = _make_source("db.schema.events")

        def my_fn(events: pd.DataFrame) -> pd.DataFrame:
            return pd.DataFrame({"x": [1]})

        trouve = PandasTrouve(transform=my_fn, inputs=[source])
        trouve.compiled = _make_compiled("mydb.myschema.mytable")

        adapter = _make_df_adapter(
            fetch_dataframes={"db.schema.events": pd.DataFrame({"col": [1]})}
        )

        result = _run_pandas_trouve(trouve, adapter)

        assert result.status == RunStatus.SUCCESS
        call_kwargs = adapter.write_dataframe.call_args.kwargs
        assert call_kwargs["database_name"] == "mydb"
        assert call_kwargs["schema_name"] == "myschema"
        assert call_kwargs["table_name"] == "mytable"
        assert call_kwargs["full_name"] == "mydb.myschema.mytable"

    def test_full_name_with_wrong_part_count_fails(self):
        source = _make_source("db.schema.events")

        def my_fn(events: pd.DataFrame) -> pd.DataFrame:
            return pd.DataFrame({"x": [1]})

        trouve = PandasTrouve(transform=my_fn, inputs=[source])
        trouve.compiled = _make_compiled("only_two_parts.table")

        adapter = _make_df_adapter(
            fetch_dataframes={"db.schema.events": pd.DataFrame({"col": [1]})}
        )

        result = _run_pandas_trouve(trouve, adapter)

        assert result.status == RunStatus.FAILURE
        assert "cannot divide the full_name" in result.error


class TestRunPandasTrouveTransformErrors:
    def test_transform_raises_results_in_failure(self):
        source = _make_source("db.schema.events")

        def bad_fn(events: pd.DataFrame) -> pd.DataFrame:
            raise ValueError("something went wrong")

        trouve = PandasTrouve(transform=bad_fn, inputs=[source])
        trouve.compiled = _make_compiled("db.schema.summary")

        adapter = _make_df_adapter(
            fetch_dataframes={"db.schema.events": pd.DataFrame({"col": [1]})}
        )

        result = _run_pandas_trouve(trouve, adapter)

        assert result.status == RunStatus.FAILURE
        assert "The transform function failed" in result.error
        assert "something went wrong" in result.error

    def test_transform_returns_non_dataframe_results_in_failure(self):
        source = _make_source("db.schema.events")

        def bad_fn(events: pd.DataFrame) -> pd.DataFrame:
            return {"not": "a dataframe"}  # type: ignore

        trouve = PandasTrouve(transform=bad_fn, inputs=[source])
        trouve.compiled = _make_compiled("db.schema.summary")

        adapter = _make_df_adapter(
            fetch_dataframes={"db.schema.events": pd.DataFrame({"col": [1]})}
        )

        result = _run_pandas_trouve(trouve, adapter)

        assert result.status == RunStatus.FAILURE
        assert "must return a pandas DataFrame" in result.error
        assert "dict" in result.error

    def test_transform_returns_none_results_in_failure(self):
        source = _make_source("db.schema.events")

        def bad_fn(events: pd.DataFrame) -> pd.DataFrame:
            return None  # type: ignore

        trouve = PandasTrouve(transform=bad_fn, inputs=[source])
        trouve.compiled = _make_compiled("db.schema.summary")

        adapter = _make_df_adapter(
            fetch_dataframes={"db.schema.events": pd.DataFrame({"col": [1]})}
        )

        result = _run_pandas_trouve(trouve, adapter)

        assert result.status == RunStatus.FAILURE
        assert "must return a pandas DataFrame" in result.error


class TestRunPandasTrouveFetchErrors:
    def test_fetch_failure_results_in_failure(self):
        source = _make_source("db.schema.events")

        def my_fn(events: pd.DataFrame) -> pd.DataFrame:
            return events

        trouve = PandasTrouve(transform=my_fn, inputs=[source])
        trouve.compiled = _make_compiled("db.schema.summary")

        adapter = _make_df_adapter(
            fetch_side_effect=RuntimeError("Snowflake connection lost")
        )

        result = _run_pandas_trouve(trouve, adapter)

        assert result.status == RunStatus.FAILURE
        assert "cannot read the input" in result.error
        assert "events" in result.error


class TestRunPandasTrouveWriteErrors:
    def test_write_exception_results_in_failure(self):
        source = _make_source("db.schema.events")

        def my_fn(events: pd.DataFrame) -> pd.DataFrame:
            return pd.DataFrame({"x": [1]})

        trouve = PandasTrouve(transform=my_fn, inputs=[source])
        trouve.compiled = _make_compiled("db.schema.summary")

        adapter = _make_df_adapter(
            fetch_dataframes={"db.schema.events": pd.DataFrame({"col": [1]})},
            write_side_effect=RuntimeError("Write failed"),
        )

        result = _run_pandas_trouve(trouve, adapter)

        assert result.status == RunStatus.FAILURE
        assert "cannot write the DataFrame" in result.error

    def test_write_returns_success_false_results_in_failure(self):
        source = _make_source("db.schema.events")

        def my_fn(events: pd.DataFrame) -> pd.DataFrame:
            return pd.DataFrame({"x": [1]})

        trouve = PandasTrouve(transform=my_fn, inputs=[source])
        trouve.compiled = _make_compiled("db.schema.summary")

        adapter = _make_df_adapter(
            fetch_dataframes={"db.schema.events": pd.DataFrame({"col": [1]})},
            write_success=False,
        )

        result = _run_pandas_trouve(trouve, adapter)

        assert result.status == RunStatus.FAILURE
        assert result.error



class TestRunPandasTrouveResultFields:
    def test_success_result_has_duration(self):
        source = _make_source("db.schema.events")

        def my_fn(events: pd.DataFrame) -> pd.DataFrame:
            return pd.DataFrame({"x": [1]})

        trouve = PandasTrouve(transform=my_fn, inputs=[source])
        trouve.compiled = _make_compiled("db.schema.summary")

        adapter = _make_df_adapter(
            fetch_dataframes={"db.schema.events": pd.DataFrame({"col": [1]})}
        )

        result = _run_pandas_trouve(trouve, adapter)

        assert result.status == RunStatus.SUCCESS
        assert result.duration_seconds >= 0.0

    def test_failure_result_has_duration(self):
        source = _make_source("db.schema.events")

        def my_fn(events: pd.DataFrame) -> pd.DataFrame:
            raise ValueError("oops")

        trouve = PandasTrouve(transform=my_fn, inputs=[source])
        trouve.compiled = _make_compiled("db.schema.summary")

        adapter = _make_df_adapter(
            fetch_side_effect=RuntimeError("fetch failed")
        )

        result = _run_pandas_trouve(trouve, adapter)

        assert result.status == RunStatus.FAILURE
        assert result.duration_seconds >= 0.0
