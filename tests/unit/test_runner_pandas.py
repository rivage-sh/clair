"""The tests of _run_dataframe_trouve, the DataFrame path of the runner."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from clair.core.runner import RunStatus, _run_dataframe_trouve
from clair.environments.routing import TrouveAddress
from clair.exceptions import InvalidTrouveAddressError
from clair.trouves.config import ResolvedConfig
from clair.trouves.pandas_trouve import PandasTrouve
from clair.trouves.trouve import CompiledAttributes, ExecutionType, Trouve, TrouveType
from tests.helpers import RecordingAdapter


def _make_compiled(physical_address: str = "db.schema.table") -> CompiledAttributes:
    return CompiledAttributes(
        physical_address=TrouveAddress.parse(physical_address),
        logical_address=TrouveAddress.parse(physical_address),
        resolved_sql="",
        file_path=Path(f"/fake/{physical_address.replace('.', '/')}.py"),
        module_name=physical_address,
        imports=[],
        config=ResolvedConfig(),
        execution_type=ExecutionType.PANDAS,
    )


def _compile_pandas(trouve: PandasTrouve, physical_address: str) -> None:
    """Compile a PandasTrouve, as discovery does.

    input_addresses holds the address that each input reads. Discovery writes
    the logical address, and recompile_for_selection() changes it to a physical
    address for each input that the run builds. These tests give one address to
    each input, thus the two addresses are equal.
    """
    trouve.compiled = _make_compiled(physical_address)
    trouve.compiled.input_addresses = [
        str(upstream.physical_address) for upstream in trouve.inputs
    ]


def _make_source(physical_address: str = "db.schema.source") -> Trouve:
    source = Trouve(type=TrouveType.SOURCE)
    source.compiled = _make_compiled(physical_address)
    return source


class TestRunPandasTrouveHappyPath:
    def test_transform_called_and_result_written(self):
        source = _make_source("db.schema.events")
        input_df = pd.DataFrame({"event_type": ["a", "b"], "count": [1, 2]})
        result_df = pd.DataFrame({"summary": [3]})

        def my_fn(events: pd.DataFrame) -> pd.DataFrame:
            return result_df

        trouve = PandasTrouve(transform=my_fn, inputs=[source])
        _compile_pandas(trouve, "db.schema.summary")

        adapter = RecordingAdapter(dataframes={"db.schema.events": input_df})

        result = _run_dataframe_trouve(trouve, adapter, trouve.physical_address)

        assert result.status == RunStatus.SUCCESS
        assert result.physical_address == "db.schema.summary"
        assert len(adapter.written_addresses) == 1
        written = adapter.dataframes[adapter.written_addresses[0]]
        pd.testing.assert_frame_equal(written, result_df)

    def test_fetch_called_for_each_input(self):
        source_a = _make_source("db.schema.a")
        source_b = _make_source("db.schema.b")
        df_a = pd.DataFrame({"x": [1]})
        df_b = pd.DataFrame({"y": [2]})

        def my_fn(a: pd.DataFrame, b: pd.DataFrame) -> pd.DataFrame:
            return pd.DataFrame({"z": [3]})

        trouve = PandasTrouve(transform=my_fn, inputs=[source_a, source_b])
        _compile_pandas(trouve, "db.schema.output")

        adapter = RecordingAdapter(
            dataframes={"db.schema.a": df_a, "db.schema.b": df_b}
        )

        result = _run_dataframe_trouve(trouve, adapter, trouve.physical_address)

        assert result.status == RunStatus.SUCCESS
        assert len(adapter.fetched_addresses) == 2

    def test_transform_receives_the_fetched_dataframe(self):
        source = _make_source("db.schema.events")
        input_df = pd.DataFrame({"col": [1]})
        received_kwargs = {}

        def my_fn(events: pd.DataFrame) -> pd.DataFrame:
            received_kwargs["events"] = events
            return pd.DataFrame({"out": [1]})

        trouve = PandasTrouve(transform=my_fn, inputs=[source])
        _compile_pandas(trouve, "db.schema.summary")

        adapter = RecordingAdapter(dataframes={"db.schema.events": input_df})
        _run_dataframe_trouve(trouve, adapter, trouve.physical_address)

        assert "events" in received_kwargs
        pd.testing.assert_frame_equal(received_kwargs["events"], input_df)


class TestRunPandasTrouveAddress:
    def test_database_schema_table_parsed_correctly(self):
        source = _make_source("db.schema.events")

        def my_fn(events: pd.DataFrame) -> pd.DataFrame:
            return pd.DataFrame({"x": [1]})

        trouve = PandasTrouve(transform=my_fn, inputs=[source])
        _compile_pandas(trouve, "mydb.myschema.mytable")

        adapter = RecordingAdapter(
            dataframes={"db.schema.events": pd.DataFrame({"col": [1]})}
        )

        result = _run_dataframe_trouve(trouve, adapter, trouve.physical_address)

        assert result.status == RunStatus.SUCCESS
        assert adapter.written_addresses == ["mydb.myschema.mytable"]

    def test_a_name_that_is_not_three_parts_never_reaches_the_write(self):
        """TrouveAddress rejects the name, so the write path holds no test for it."""
        with pytest.raises(InvalidTrouveAddressError):
            TrouveAddress.parse("only_two_parts.table")


class TestRunPandasTrouveTransformErrors:
    def test_transform_raises_results_in_failure(self):
        source = _make_source("db.schema.events")

        def bad_fn(events: pd.DataFrame) -> pd.DataFrame:
            raise ValueError("something went wrong")

        trouve = PandasTrouve(transform=bad_fn, inputs=[source])
        _compile_pandas(trouve, "db.schema.summary")

        adapter = RecordingAdapter(
            dataframes={"db.schema.events": pd.DataFrame({"col": [1]})}
        )

        result = _run_dataframe_trouve(trouve, adapter, trouve.physical_address)

        assert result.status == RunStatus.FAILURE
        assert "Clair cannot build the DataFrame" in result.error
        assert "something went wrong" in result.error

    def test_transform_returns_non_dataframe_results_in_failure(self):
        source = _make_source("db.schema.events")

        def bad_fn(events: pd.DataFrame) -> pd.DataFrame:
            return {"not": "a dataframe"}  # type: ignore

        trouve = PandasTrouve(transform=bad_fn, inputs=[source])
        _compile_pandas(trouve, "db.schema.summary")

        adapter = RecordingAdapter(
            dataframes={"db.schema.events": pd.DataFrame({"col": [1]})}
        )

        result = _run_dataframe_trouve(trouve, adapter, trouve.physical_address)

        assert result.status == RunStatus.FAILURE
        assert "must give a pandas DataFrame" in result.error
        assert "dict" in result.error

    def test_transform_returns_none_results_in_failure(self):
        source = _make_source("db.schema.events")

        def bad_fn(events: pd.DataFrame) -> pd.DataFrame:
            return None  # type: ignore

        trouve = PandasTrouve(transform=bad_fn, inputs=[source])
        _compile_pandas(trouve, "db.schema.summary")

        adapter = RecordingAdapter(
            dataframes={"db.schema.events": pd.DataFrame({"col": [1]})}
        )

        result = _run_dataframe_trouve(trouve, adapter, trouve.physical_address)

        assert result.status == RunStatus.FAILURE
        assert "must give a pandas DataFrame" in result.error


class TestRunPandasTrouveFetchErrors:
    def test_fetch_failure_results_in_failure(self):
        source = _make_source("db.schema.events")

        def my_fn(events: pd.DataFrame) -> pd.DataFrame:
            return events

        trouve = PandasTrouve(transform=my_fn, inputs=[source])
        _compile_pandas(trouve, "db.schema.summary")

        adapter = RecordingAdapter(
            fetch_error=RuntimeError("Snowflake connection lost")
        )

        result = _run_dataframe_trouve(trouve, adapter, trouve.physical_address)

        assert result.status == RunStatus.FAILURE
        assert "cannot read the input" in result.error
        assert "events" in result.error


class TestRunPandasTrouveWriteErrors:
    def test_write_exception_results_in_failure(self):
        source = _make_source("db.schema.events")

        def my_fn(events: pd.DataFrame) -> pd.DataFrame:
            return pd.DataFrame({"x": [1]})

        trouve = PandasTrouve(transform=my_fn, inputs=[source])
        _compile_pandas(trouve, "db.schema.summary")

        adapter = RecordingAdapter(
            dataframes={"db.schema.events": pd.DataFrame({"col": [1]})},
            write_error=RuntimeError("Write failed"),
        )

        result = _run_dataframe_trouve(trouve, adapter, trouve.physical_address)

        assert result.status == RunStatus.FAILURE
        assert "cannot write the DataFrame" in result.error

    def test_write_returns_success_false_results_in_failure(self):
        source = _make_source("db.schema.events")

        def my_fn(events: pd.DataFrame) -> pd.DataFrame:
            return pd.DataFrame({"x": [1]})

        trouve = PandasTrouve(transform=my_fn, inputs=[source])
        _compile_pandas(trouve, "db.schema.summary")

        adapter = RecordingAdapter(
            dataframes={"db.schema.events": pd.DataFrame({"col": [1]})},
            fail_on=["-- write_dataframe"],
        )

        result = _run_dataframe_trouve(trouve, adapter, trouve.physical_address)

        assert result.status == RunStatus.FAILURE
        assert result.error



class TestRunPandasTrouveResultFields:
    def test_success_result_has_duration(self):
        source = _make_source("db.schema.events")

        def my_fn(events: pd.DataFrame) -> pd.DataFrame:
            return pd.DataFrame({"x": [1]})

        trouve = PandasTrouve(transform=my_fn, inputs=[source])
        _compile_pandas(trouve, "db.schema.summary")

        adapter = RecordingAdapter(
            dataframes={"db.schema.events": pd.DataFrame({"col": [1]})}
        )

        result = _run_dataframe_trouve(trouve, adapter, trouve.physical_address)

        assert result.status == RunStatus.SUCCESS
        assert result.duration_seconds >= 0.0

    def test_failure_result_has_duration(self):
        source = _make_source("db.schema.events")

        def my_fn(events: pd.DataFrame) -> pd.DataFrame:
            raise ValueError("oops")

        trouve = PandasTrouve(transform=my_fn, inputs=[source])
        _compile_pandas(trouve, "db.schema.summary")

        adapter = RecordingAdapter(
            fetch_error=RuntimeError("fetch failed")
        )

        result = _run_dataframe_trouve(trouve, adapter, trouve.physical_address)

        assert result.status == RunStatus.FAILURE
        assert result.duration_seconds >= 0.0
