"""Tests for the `clair validate` command."""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest
from click.testing import CliRunner

from clair.cli.main import cli


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


def _write_routing(project_dir: Path, body: str) -> None:
    (project_dir / "__routing__.py").write_text(textwrap.dedent(body))


def _run_validate(project_dir: Path, *args: str):
    return CliRunner().invoke(cli, ["validate", "--project", str(project_dir), *args])


class TestValidateSucceeds:
    def test_no_routing_file_passes(self, project_with_trouves: Path):
        result = _run_validate(project_with_trouves)
        assert result.exit_code == 0
        assert "Every routed name is valid" in result.output

    def test_valid_callable_rule_passes(self, project_with_trouves: Path):
        _write_routing(project_with_trouves, '''
            routing = {
                "dev": lambda database_name, schema_name, table_name: (
                    f"{database_name}_dev.{schema_name}.{table_name}"
                ),
            }
        ''')
        result = _run_validate(project_with_trouves)
        assert result.exit_code == 0
        assert "Every routed name is valid" in result.output

    def test_output_names_the_environment_and_rule(self, project_with_trouves: Path):
        _write_routing(project_with_trouves, '''
            routing = {
                "dev": lambda database_name, schema_name, table_name: (
                    f"{database_name}_dev.{schema_name}.{table_name}"
                ),
            }
        ''')
        result = _run_validate(project_with_trouves)
        assert "environment: dev" in result.output
        # The rule description shows the source, not the word "lambda" alone.
        assert "_dev" in result.output

    def test_counts_only_routable_trouves(self, project_with_trouves: Path):
        # 2 TABLE Trouves route. The SOURCE Trouve never routes.
        result = _run_validate(project_with_trouves)
        assert "Trouves to route: 2" in result.output


class TestValidateFails:
    def test_invalid_identifier_fails(self, project_with_trouves: Path):
        _write_routing(project_with_trouves, '''
            routing = {
                "dev": lambda database_name, schema_name, table_name: (
                    f"{database_name}-dev.{schema_name}.{table_name}"
                ),
            }
        ''')
        result = _run_validate(project_with_trouves)
        assert result.exit_code == 1
        assert "invalid database_name" in result.output

    def test_reports_every_bad_trouve_not_only_the_first(self, project_with_trouves: Path):
        _write_routing(project_with_trouves, '''
            routing = {
                "dev": lambda database_name, schema_name, table_name: (
                    f"{database_name}-dev.{schema_name}.{table_name}"
                ),
            }
        ''')
        result = _run_validate(project_with_trouves)
        assert "analytics.finance.revenue" in result.output
        assert "analytics.reports.revenue" in result.output
        assert "2 problems found" in result.output

    def test_collision_fails(self, project_with_trouves: Path):
        _write_routing(project_with_trouves, '''
            routing = {
                "dev": lambda database_name, schema_name, table_name: (
                    f"DEV.shared.{table_name}"
                ),
            }
        ''')
        result = _run_validate(project_with_trouves)
        assert result.exit_code == 1
        assert "DEV.shared.revenue" in result.output
        assert "analytics.finance.revenue" in result.output
        assert "analytics.reports.revenue" in result.output

    def test_rule_that_raises_fails(self, project_with_trouves: Path, monkeypatch):
        monkeypatch.delenv("CLAIR_USER", raising=False)
        _write_routing(project_with_trouves, '''
            import os

            routing = {
                "dev": lambda database_name, schema_name, table_name: (
                    f"{database_name}_{os.environ['CLAIR_USER']}"
                    f".{schema_name}.{table_name}"
                ),
            }
        ''')
        result = _run_validate(project_with_trouves)
        assert result.exit_code == 1
        assert "CLAIR_USER" in result.output

    def test_broken_routing_file_fails(self, project_with_trouves: Path):
        _write_routing(project_with_trouves, "routing = {\n")
        result = _run_validate(project_with_trouves)
        assert result.exit_code == 1


class TestUnnamedEnvironmentWarning:
    def test_absent_environment_warns(self, project_with_trouves: Path):
        _write_routing(project_with_trouves, '''
            routing = {"dev": None}
        ''')
        result = _run_validate(project_with_trouves, "--env", "typo")
        assert "does not name the environment 'typo'" in result.output

    def test_explicit_none_does_not_warn(self, project_with_trouves: Path):
        """A "prod": None entry is a decision, so it must stay quiet."""
        _write_routing(project_with_trouves, '''
            routing = {"dev": None, "prod": None}
        ''')
        result = _run_validate(project_with_trouves, "--env", "prod")
        assert result.exit_code == 0
        assert "does not name the environment" not in result.output

    def test_env_var_selects_the_environment(self, project_with_trouves: Path, monkeypatch):
        monkeypatch.setenv("CLAIR_ENV", "staging")
        _write_routing(project_with_trouves, '''
            routing = {"staging": None}
        ''')
        result = _run_validate(project_with_trouves)
        assert "environment: staging" in result.output
