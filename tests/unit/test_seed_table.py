"""The tests of SeedTable: the model, the discovery, and the compiler."""

from __future__ import annotations

import textwrap
from pathlib import Path

import pandas as pd
import pytest
from pydantic import ValidationError

from clair.core.compiler import write_compile_output
from clair.core.dag import build_dag, get_executable_nodes
from clair.core.discovery import discover_project
from clair.trouves.run_config import Append, Incremental
from clair.trouves.seed_table import SeedTable
from clair.trouves.trouve import ExecutionType, TrouveType

FAKE_RUN_ID = "0" * 32


def _frame() -> pd.DataFrame:
    return pd.DataFrame({"code": ["US", "FR"], "rate": [0.0, 0.20]})


class TestSeedTableModel:
    def test_the_seed_gives_its_own_dataframe(self):
        frame = _frame()
        trouve = SeedTable(dataframe=frame)

        pd.testing.assert_frame_equal(trouve.build_dataframe(), frame)

    def test_the_seed_reads_no_other_trouve(self):
        trouve = SeedTable(dataframe=_frame())

        assert trouve.upstream_trouves() == []
        assert trouve.parameter_names() == []

    def test_the_seed_runs_on_the_dataframe_backend(self):
        trouve = SeedTable(dataframe=_frame())

        assert trouve.execution_type == ExecutionType.PANDAS
        assert trouve.type == TrouveType.TABLE

    def test_the_source_text_holds_the_dtypes_and_the_rows(self):
        trouve = SeedTable(dataframe=_frame())

        source_text = trouve.source_text()

        assert "2 row(s), 2 column(s)" in source_text
        assert "code: object" in source_text
        assert "rate: float64" in source_text
        assert "US" in source_text

    def test_the_source_file_is_none(self):
        """A seed has no function, thus the artifact needs no import section."""
        assert SeedTable(dataframe=_frame()).source_file() is None


class TestSeedTableValidation:
    @pytest.mark.parametrize(
        "arguments",
        [{"type": TrouveType.VIEW}, {"run_config": Incremental(strategy=Append())}],
        ids=["a_type", "a_run_config"],
    )
    def test_a_keyword_of_a_different_class_is_rejected(self, arguments):
        """A seed is always a table that clair makes again, thus it reads neither keyword."""
        with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
            SeedTable(dataframe=_frame(), **arguments)

    def test_a_dataframe_with_no_column_is_rejected(self):
        with pytest.raises(ValidationError, match="one column minimum"):
            SeedTable(dataframe=pd.DataFrame())

    def test_a_column_name_that_is_not_text_is_rejected(self):
        with pytest.raises(ValidationError, match="must be a string"):
            SeedTable(dataframe=pd.DataFrame({0: [1], "code": ["US"]}))

    def test_a_duplicate_column_name_is_rejected(self):
        frame = pd.DataFrame([[1, 2]], columns=pd.Index(["code", "code"]))

        with pytest.raises(ValidationError, match="duplicate column name"):
            SeedTable(dataframe=frame)

    def test_a_seed_with_no_row_is_accepted(self):
        """A seed with no row is valid. The table exists, and it holds no row."""
        frame = pd.DataFrame({"code": pd.Series(dtype="string")})

        trouve = SeedTable(dataframe=frame)

        assert len(trouve.build_dataframe()) == 0


def _make_seed_project(tmp_path: Path) -> Path:
    """Make a project with a seed and a SQL Trouve that reads the seed.

    The structure is:
        mydb/reference/tax_rates.py    [a SeedTable]
        mydb/derived/orders.py         [a Trouve] reads reference.tax_rates
    """
    (tmp_path / "mydb" / "reference").mkdir(parents=True)
    (tmp_path / "mydb" / "derived").mkdir(parents=True)

    (tmp_path / "mydb" / "reference" / "tax_rates.py").write_text(textwrap.dedent("""\
        import pandas as pd
        from clair import Column, ColumnType, SeedTable

        rates = pd.DataFrame({"country_code": ["US", "FR"], "tax_rate": [0.0, 0.20]})
        rates["country_code"] = rates["country_code"].astype("string")

        trouve = SeedTable(
            dataframe=rates,
            docs="The tax rate of each country.",
            columns=[Column(name="country_code", type=ColumnType.VARCHAR)],
        )
    """))

    (tmp_path / "mydb" / "derived" / "orders.py").write_text(textwrap.dedent("""\
        from clair import Table
        from mydb.reference.tax_rates import trouve as tax_rates

        trouve = Table(sql=f"SELECT * FROM {tax_rates}")
    """))

    return tmp_path


class TestSeedTableDiscovery:
    def test_discovery_finds_the_seed(self, tmp_path: Path):
        project_root = _make_seed_project(tmp_path)

        trouves = discover_project(project_root)

        seeds = [trouve for trouve in trouves if isinstance(trouve, SeedTable)]
        assert len(seeds) == 1
        assert seeds[0].compiled is not None
        assert str(seeds[0].compiled.logical_address) == "mydb.reference.tax_rates"

    def test_the_seed_compiles_with_no_input_address(self, tmp_path: Path):
        project_root = _make_seed_project(tmp_path)

        trouves = discover_project(project_root)
        seed = next(trouve for trouve in trouves if isinstance(trouve, SeedTable))

        assert seed.compiled is not None
        assert seed.compiled.execution_type == ExecutionType.PANDAS
        assert seed.compiled.input_addresses == []
        assert seed.compiled.resolved_sql == ""
        assert "country_code: string" in seed.compiled.resolved_transform

    def test_a_sql_trouve_reads_the_seed(self, tmp_path: Path):
        """A seed is an ordinary upstream. The token becomes its address."""
        project_root = _make_seed_project(tmp_path)

        trouves = discover_project(project_root)
        orders = next(
            trouve for trouve in trouves
            if trouve.compiled
            and str(trouve.compiled.logical_address).endswith("orders")
        )

        assert orders.compiled is not None
        assert "mydb.reference.tax_rates" in orders.compiled.resolved_sql


class TestSeedTableCompiler:
    def test_compile_writes_the_rows_to_the_artifact(self, tmp_path: Path):
        project_root = _make_seed_project(tmp_path)

        dag = build_dag(discover_project(project_root))
        selected = get_executable_nodes(dag)

        output = write_compile_output(
            dag, selected, tmp_path, run_id=FAKE_RUN_ID
        )

        seed_node = next(
            node for node in output.compiled_nodes
            if node.addresses.physical.table_name.lower() == "tax_rates"
        )
        assert seed_node.execution_type == ExecutionType.PANDAS
        assert seed_node.sql == []
        assert seed_node.artifact_path is not None

        artifact_text = Path(seed_node.artifact_path).read_text()
        assert "execution_type: pandas" in artifact_text
        assert "country_code" in artifact_text
        assert "US" in artifact_text
