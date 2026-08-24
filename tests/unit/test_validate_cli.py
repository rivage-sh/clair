"""Tests for the `clair validate` command."""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest
from click.testing import CliRunner

from clair.cli.main import cli

# Every routing file in these tests needs an entry class. This prelude gives one.
_PRELUDE = """\
import os

from clair import RoutingEntry, RoutingTable, TrouveAddress


"""


@pytest.fixture
def project_with_trouves(tmp_path: Path) -> Path:
    """Build a project with one SOURCE Trouve and two TABLE Trouves."""
    project_dir = tmp_path / "proj"
    (project_dir / "source" / "raw").mkdir(parents=True)
    (project_dir / "source" / "raw" / "orders.py").write_text(
        "from clair import Trouve, TrouveType\n\n"
        "trouve = Trouve(type=TrouveType.SOURCE)\n"
    )
    for schema_name in ("finance", "reports"):
        (project_dir / "analytics" / schema_name).mkdir(parents=True)
        (project_dir / "analytics" / schema_name / "revenue.py").write_text(
            "from clair import Trouve, TrouveType\n\n"
            'trouve = Trouve(type=TrouveType.TABLE, sql="SELECT 1 AS x")\n'
        )
    return project_dir


def _write_routing(project_dir: Path, body: str, prelude: bool = True) -> None:
    content = textwrap.dedent(body)
    (project_dir / "__routing__.py").write_text(
        _PRELUDE + content if prelude else content
    )


def _run_validate(project_dir: Path, *args: str):
    return CliRunner().invoke(cli, ["validate", "--project", str(project_dir), *args])


# A routing entry that adds "_dev" to the database name.
_DEV_SUFFIX_ENTRY = '''
    class DevRouting(RoutingEntry):
        environment_name: str = "dev"

        def route(self, trouve_address: TrouveAddress) -> TrouveAddress:
            return trouve_address.model_copy(update={
                "database_name": f"{trouve_address.database_name}_dev"
            })


    routing = RoutingTable(entries=[DevRouting()])
'''


class TestValidateSucceeds:
    def test_no_routing_file_passes(self, project_with_trouves: Path):
        result = _run_validate(project_with_trouves)
        assert result.exit_code == 0
        assert "Every physical address is valid" in result.output

    def test_a_valid_entry_passes(self, project_with_trouves: Path):
        _write_routing(project_with_trouves, _DEV_SUFFIX_ENTRY)
        result = _run_validate(project_with_trouves)
        assert result.exit_code == 0
        assert "Every physical address is valid" in result.output

    def test_output_names_the_environment_and_the_entry(
        self, project_with_trouves: Path
    ):
        _write_routing(project_with_trouves, _DEV_SUFFIX_ENTRY)
        result = _run_validate(project_with_trouves)
        assert "environment: dev" in result.output
        # The description names the class, not the base class.
        assert "DevRouting" in result.output

    def test_counts_only_the_routable_trouves(self, project_with_trouves: Path):
        # 2 TABLE Trouves route. The SOURCE Trouve never routes.
        result = _run_validate(project_with_trouves)
        assert "Trouves to route: 2" in result.output


class TestValidateFails:
    def test_an_invalid_identifier_fails(self, project_with_trouves: Path):
        _write_routing(project_with_trouves, '''
            class DashRouting(RoutingEntry):
                environment_name: str = "dev"

                def route(self, trouve_address: TrouveAddress) -> TrouveAddress:
                    return TrouveAddress(
                        database_name=f"{trouve_address.database_name}-dev",
                        schema_name=trouve_address.schema_name,
                        table_name=trouve_address.table_name,
                    )


            routing = RoutingTable(entries=[DashRouting()])
        ''')
        result = _run_validate(project_with_trouves)
        assert result.exit_code == 1
        assert "not a valid identifier" in result.output

    def test_reports_every_bad_trouve_not_only_the_first(
        self, project_with_trouves: Path
    ):
        _write_routing(project_with_trouves, '''
            class DashRouting(RoutingEntry):
                environment_name: str = "dev"

                def route(self, trouve_address: TrouveAddress) -> TrouveAddress:
                    return TrouveAddress(
                        database_name=f"{trouve_address.database_name}-dev",
                        schema_name=trouve_address.schema_name,
                        table_name=trouve_address.table_name,
                    )


            routing = RoutingTable(entries=[DashRouting()])
        ''')
        result = _run_validate(project_with_trouves)
        assert "analytics.finance.revenue" in result.output
        assert "analytics.reports.revenue" in result.output
        assert "2 problems found" in result.output

    def test_a_collision_fails(self, project_with_trouves: Path):
        _write_routing(project_with_trouves, '''
            class SharedRouting(RoutingEntry):
                environment_name: str = "dev"

                def route(self, trouve_address: TrouveAddress) -> TrouveAddress:
                    return trouve_address.model_copy(update={
                        "database_name": "DEV", "schema_name": "shared"
                    })


            routing = RoutingTable(entries=[SharedRouting()])
        ''')
        result = _run_validate(project_with_trouves)
        assert result.exit_code == 1
        assert "DEV.shared.revenue" in result.output
        assert "analytics.finance.revenue" in result.output
        assert "analytics.reports.revenue" in result.output

    def test_an_entry_that_raises_fails(self, project_with_trouves: Path, monkeypatch):
        monkeypatch.delenv("CLAIR_USER", raising=False)
        _write_routing(project_with_trouves, '''
            class UserRouting(RoutingEntry):
                environment_name: str = "dev"

                def route(self, trouve_address: TrouveAddress) -> TrouveAddress:
                    user_name = os.environ["CLAIR_USER"]
                    return trouve_address.model_copy(update={
                        "database_name": f"{trouve_address.database_name}_{user_name}"
                    })


            routing = RoutingTable(entries=[UserRouting()])
        ''')
        result = _run_validate(project_with_trouves)
        assert result.exit_code == 1
        assert "CLAIR_USER" in result.output

    def test_a_broken_routing_file_fails(self, project_with_trouves: Path):
        _write_routing(project_with_trouves, "routing = [\n", prelude=False)
        result = _run_validate(project_with_trouves)
        assert result.exit_code == 1

    def test_a_duplicate_environment_name_fails(self, project_with_trouves: Path):
        _write_routing(project_with_trouves, '''
            class DevRouting(RoutingEntry):
                environment_name: str = "dev"

                def route(self, trouve_address: TrouveAddress) -> TrouveAddress:
                    return trouve_address


            routing = RoutingTable(entries=[DevRouting(), DevRouting()])
        ''')
        result = _run_validate(project_with_trouves)
        assert result.exit_code == 1

    def test_a_bad_directory_name_fails(self, tmp_path: Path):
        """A directory that Snowflake cannot use as a name stops validate."""
        project_dir = tmp_path / "proj"
        (project_dir / "my-db" / "finance").mkdir(parents=True)
        (project_dir / "my-db" / "finance" / "revenue.py").write_text(
            "from clair import Trouve, TrouveType\n\n"
            'trouve = Trouve(type=TrouveType.TABLE, sql="SELECT 1 AS x")\n'
        )
        result = _run_validate(project_dir)
        assert result.exit_code == 1


class TestUnnamedEnvironmentWarning:
    def test_an_absent_environment_warns(self, project_with_trouves: Path):
        _write_routing(project_with_trouves, _DEV_SUFFIX_ENTRY)
        result = _run_validate(project_with_trouves, "--env", "typo")
        assert "does not name the environment 'typo'" in result.output

    def test_a_named_environment_does_not_warn(self, project_with_trouves: Path):
        """An entry for the environment is a decision, so it must stay quiet."""
        _write_routing(project_with_trouves, '''
            class ProdRouting(RoutingEntry):
                environment_name: str = "prod"

                def route(self, trouve_address: TrouveAddress) -> TrouveAddress:
                    return trouve_address


            routing = RoutingTable(entries=[ProdRouting()])
        ''')
        result = _run_validate(project_with_trouves, "--env", "prod")
        assert result.exit_code == 0
        assert "does not name the environment" not in result.output

    def test_the_env_var_selects_the_environment(
        self, project_with_trouves: Path, monkeypatch
    ):
        monkeypatch.setenv("CLAIR_ENV", "staging")
        _write_routing(project_with_trouves, '''
            class StagingRouting(RoutingEntry):
                environment_name: str = "staging"

                def route(self, trouve_address: TrouveAddress) -> TrouveAddress:
                    return trouve_address


            routing = RoutingTable(entries=[StagingRouting()])
        ''')
        result = _run_validate(project_with_trouves)
        assert "environment: staging" in result.output
