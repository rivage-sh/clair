"""Tests for the project __routing__.py loader."""

from __future__ import annotations

import os
import textwrap
from pathlib import Path

import pytest

from clair.environments.project_routing import (
    ROUTING_FILE_NAME,
    load_project_routing,
)
from clair.environments.routing import route
from clair.exceptions import InvalidRoutingFileError
from clair.trouves.trouve import TrouveType

# A routing file needs an entry class. This prelude gives the tests one.
_PRELUDE = """\
import os

from clair import RoutingEntry, RoutingTable, TrouveAddress


class DatabaseOverride(RoutingEntry):
    environment_name: str = "dev"
    database_name: str

    def route(
        self, trouve_address: TrouveAddress, trouve_type: TrouveType
    ) -> TrouveAddress:
        return trouve_address.model_copy(update={"database_name": self.database_name})


"""


def _write_routing_file(project_dir: Path, body: str) -> Path:
    """Write a routing file that holds only the given body."""
    path = project_dir / ROUTING_FILE_NAME
    path.write_text(textwrap.dedent(body))
    return path


def _write_with_prelude(project_dir: Path, body: str) -> Path:
    """Write a routing file that holds the entry prelude and the given body."""
    path = project_dir / ROUTING_FILE_NAME
    path.write_text(_PRELUDE + textwrap.dedent(body))
    return path


class TestLoadProjectRouting:
    def test_missing_file_gives_passthrough(self, routing_project: Path):
        result = load_project_routing(routing_project, "dev")
        assert result.entry is None
        assert result.file_path is None
        assert result.file_exists is False

    def test_loads_an_entry(self, routing_project: Path):
        _write_with_prelude(routing_project, '''
            routing = RoutingTable(entries=[DatabaseOverride(database_name="OMER_DEV")])
        ''')
        result = load_project_routing(routing_project, "dev")
        assert result.entry is not None
        # The entry is a user subclass, so read its own field through the model.
        assert result.entry.model_dump()["database_name"] == "OMER_DEV"
        assert str(route("a.b.c", TrouveType.TABLE, result.entry)) == "OMER_DEV.b.c"

    def test_an_empty_table_gives_passthrough(self, routing_project: Path):
        _write_with_prelude(routing_project, '''
            routing = RoutingTable(entries=[])
        ''')
        result = load_project_routing(routing_project, "dev")
        assert result.entry is None
        assert result.file_exists is True

    def test_unknown_environment_reports_the_known_names(self, routing_project: Path):
        _write_with_prelude(routing_project, '''
            routing = RoutingTable(entries=[
                DatabaseOverride(environment_name="dev", database_name="D"),
                DatabaseOverride(environment_name="staging", database_name="S"),
            ])
        ''')
        result = load_project_routing(routing_project, "typo")
        assert result.entry is None
        assert result.environment_names == ["dev", "staging"]
        assert result.is_unnamed_environment is True

    def test_a_named_environment_is_not_unnamed(self, routing_project: Path):
        _write_with_prelude(routing_project, '''
            routing = RoutingTable(entries=[DatabaseOverride(database_name="D")])
        ''')
        result = load_project_routing(routing_project, "dev")
        assert result.is_unnamed_environment is False

    def test_an_entry_reads_an_environment_variable(
        self, routing_project: Path, monkeypatch
    ):
        monkeypatch.setenv("CLAIR_USER", "obaddour")
        _write_with_prelude(routing_project, '''
            class DeveloperRouting(RoutingEntry):
                environment_name: str = "dev"

                def route(
                    self, trouve_address: TrouveAddress, trouve_type: TrouveType
                ) -> TrouveAddress:
                    user_name = os.environ["CLAIR_USER"].upper()
                    return trouve_address.model_copy(update={
                        "database_name": f"{trouve_address.database_name}_{user_name}"
                    })


            routing = RoutingTable(entries=[DeveloperRouting()])
        ''')
        result = load_project_routing(routing_project, "dev")
        physical_address = route(
            "analytics.finance.revenue", TrouveType.TABLE, result.entry
        )
        assert str(physical_address) == "analytics_OBADDOUR.finance.revenue"


class TestRoutingFileValidation:
    def test_syntax_error_raises(self, routing_project: Path):
        _write_routing_file(routing_project, "routing = [\n")
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
        with pytest.raises(InvalidRoutingFileError, match="must define a 'routing'"):
            load_project_routing(routing_project, "dev")

    def test_routing_that_is_not_a_table_raises(self, routing_project: Path):
        _write_routing_file(routing_project, "routing = 'not a table'\n")
        with pytest.raises(InvalidRoutingFileError, match="must be a RoutingTable"):
            load_project_routing(routing_project, "dev")

    def test_a_dict_routing_table_raises(self, routing_project: Path):
        """The old dict format must not pass without a word."""
        _write_routing_file(routing_project, 'routing = {"dev": None}\n')
        with pytest.raises(InvalidRoutingFileError, match="must be a RoutingTable"):
            load_project_routing(routing_project, "dev")

    def test_a_duplicate_environment_name_raises(self, routing_project: Path):
        _write_with_prelude(routing_project, '''
            routing = RoutingTable(entries=[
                DatabaseOverride(database_name="A"),
                DatabaseOverride(database_name="B"),
            ])
        ''')
        with pytest.raises(InvalidRoutingFileError, match="more than one entry"):
            load_project_routing(routing_project, "dev")


class TestRoutingFileCache:
    def test_repeated_loads_run_the_file_one_time(self, routing_project: Path):
        _write_with_prelude(routing_project, '''
            os.environ["CLAIR_TEST_LOAD_COUNT"] = str(
                int(os.environ.get("CLAIR_TEST_LOAD_COUNT", "0")) + 1
            )

            routing = RoutingTable(entries=[])
        ''')
        os.environ.pop("CLAIR_TEST_LOAD_COUNT", None)
        try:
            load_project_routing(routing_project, "dev")
            load_project_routing(routing_project, "dev")
            load_project_routing(routing_project, "dev")
            assert os.environ["CLAIR_TEST_LOAD_COUNT"] == "1"
        finally:
            os.environ.pop("CLAIR_TEST_LOAD_COUNT", None)

    def test_an_edit_reloads_the_file(self, routing_project: Path):
        _write_with_prelude(routing_project, '''
            routing = RoutingTable(entries=[DatabaseOverride(database_name="FIRST")])
        ''')
        first = load_project_routing(routing_project, "dev")
        assert first.entry is not None
        assert first.entry.model_dump()["database_name"] == "FIRST"

        _write_with_prelude(routing_project, '''
            routing = RoutingTable(entries=[DatabaseOverride(database_name="SECOND")])
        ''')
        second = load_project_routing(routing_project, "dev")
        assert second.entry is not None
        assert second.entry.model_dump()["database_name"] == "SECOND"
