"""Tests for the project __routing__.py loader."""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

from clair.environments.project_routing import (
    ROUTING_FILE_NAME,
    load_project_routing,
)
from clair.environments.routing import DatabaseOverrideRouting, route
from clair.exceptions import InvalidRoutingFileError
from clair.trouves.trouve import TrouveType


def _write_routing_file(project_dir: Path, body: str) -> Path:
    path = project_dir / ROUTING_FILE_NAME
    path.write_text(textwrap.dedent(body))
    return path


class TestLoadProjectRouting:
    def test_missing_file_gives_passthrough(self, routing_project: Path):
        result = load_project_routing(routing_project, "dev")
        assert result.rule is None
        assert result.file_path is None
        assert result.file_exists is False

    def test_loads_a_callable_rule(self, routing_project: Path):
        _write_routing_file(routing_project, '''
            routing = {
                "dev": lambda database_name, schema_name, table_name: (
                    f"{database_name}_dev.{schema_name}.{table_name}"
                ),
            }
        ''')
        result = load_project_routing(routing_project, "dev")
        assert callable(result.rule)
        assert route("refined.products.catalog", TrouveType.TABLE, result.rule) == (
            "refined_dev.products.catalog"
        )

    def test_loads_a_typed_rule(self, routing_project: Path):
        _write_routing_file(routing_project, '''
            from clair.environments.routing import DatabaseOverrideRouting

            routing = {"dev": DatabaseOverrideRouting(database_name="OMER_DEV")}
        ''')
        result = load_project_routing(routing_project, "dev")
        assert isinstance(result.rule, DatabaseOverrideRouting)
        assert result.rule.database_name == "OMER_DEV"

    def test_environment_without_a_rule_gives_passthrough(self, routing_project: Path):
        _write_routing_file(routing_project, '''
            routing = {"dev": None, "prod": None}
        ''')
        result = load_project_routing(routing_project, "prod")
        assert result.rule is None
        assert result.file_exists is True

    def test_unknown_environment_reports_the_known_names(self, routing_project: Path):
        _write_routing_file(routing_project, '''
            routing = {"dev": None, "staging": None}
        ''')
        result = load_project_routing(routing_project, "typo")
        assert result.rule is None
        assert result.environment_names == ["dev", "staging"]

    def test_rule_reads_an_environment_variable(self, routing_project: Path, monkeypatch):
        monkeypatch.setenv("CLAIR_USER", "obaddour")
        _write_routing_file(routing_project, '''
            import os

            routing = {
                "dev": lambda database_name, schema_name, table_name: (
                    f"{database_name}_{os.environ['CLAIR_USER'].upper()}"
                    f".{schema_name}.{table_name}"
                ),
            }
        ''')
        result = load_project_routing(routing_project, "dev")
        assert route("analytics.finance.revenue", TrouveType.TABLE, result.rule) == (
            "analytics_OBADDOUR.finance.revenue"
        )


class TestRoutingFileValidation:
    def test_syntax_error_raises(self, routing_project: Path):
        _write_routing_file(routing_project, "routing = {\n")
        with pytest.raises(InvalidRoutingFileError, match="SyntaxError"):
            load_project_routing(routing_project, "dev")

    def test_error_at_import_time_raises(self, routing_project: Path):
        _write_routing_file(routing_project, '''
            raise ValueError("boom")
        ''')
        with pytest.raises(InvalidRoutingFileError, match="boom"):
            load_project_routing(routing_project, "dev")

    def test_missing_routing_table_raises(self, routing_project: Path):
        _write_routing_file(routing_project, "# no routing table here\n")
        with pytest.raises(InvalidRoutingFileError, match="'routing' dict"):
            load_project_routing(routing_project, "dev")

    def test_routing_table_not_a_dict_raises(self, routing_project: Path):
        _write_routing_file(routing_project, "routing = 'not a dict'\n")
        with pytest.raises(InvalidRoutingFileError, match="must be a dict"):
            load_project_routing(routing_project, "dev")

    def test_rule_of_wrong_type_raises(self, routing_project: Path):
        _write_routing_file(routing_project, '''
            routing = {"dev": "OMER_DEV"}
        ''')
        with pytest.raises(InvalidRoutingFileError, match="must "):
            load_project_routing(routing_project, "dev")

    def test_non_string_key_raises(self, routing_project: Path):
        _write_routing_file(routing_project, '''
            routing = {1: None}
        ''')
        with pytest.raises(InvalidRoutingFileError, match="environment name string"):
            load_project_routing(routing_project, "dev")


class TestRoutingFileCache:
    def test_repeated_loads_run_the_file_one_time(self, routing_project: Path):
        _write_routing_file(routing_project, '''
            import os

            os.environ["CLAIR_TEST_LOAD_COUNT"] = str(
                int(os.environ.get("CLAIR_TEST_LOAD_COUNT", "0")) + 1
            )

            routing = {"dev": None}
        ''')
        import os

        os.environ.pop("CLAIR_TEST_LOAD_COUNT", None)
        try:
            load_project_routing(routing_project, "dev")
            load_project_routing(routing_project, "dev")
            load_project_routing(routing_project, "dev")
            assert os.environ["CLAIR_TEST_LOAD_COUNT"] == "1"
        finally:
            os.environ.pop("CLAIR_TEST_LOAD_COUNT", None)

    def test_an_edit_reloads_the_file(self, routing_project: Path):
        _write_routing_file(routing_project, '''
            from clair.environments.routing import DatabaseOverrideRouting

            routing = {"dev": DatabaseOverrideRouting(database_name="FIRST")}
        ''')
        first = load_project_routing(routing_project, "dev")
        assert first.rule.database_name == "FIRST"

        _write_routing_file(routing_project, '''
            from clair.environments.routing import DatabaseOverrideRouting

            routing = {"dev": DatabaseOverrideRouting(database_name="SECOND")}
        ''')
        second = load_project_routing(routing_project, "dev")
        assert second.rule.database_name == "SECOND"
