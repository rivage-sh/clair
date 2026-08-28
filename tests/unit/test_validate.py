"""The tests of `clair validate`, through the Python API.

``clair.validate()`` gives a ValidationReport. Each test reads the lists of that
report, thus a test names the Trouve that has the problem, and it parses no
text. One class at the end covers the exit code of the CLI.
"""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest
from click.testing import CliRunner

import clair
from clair.cli.main import cli
from clair.core.text_references import TextReferenceLocation
from clair.core.validation import ValidationReport
from clair.exceptions import ClairError

# Every routing file in these tests needs an entry class. This prelude gives one.
_PRELUDE = """\
import os

from clair import RoutingEntry, RoutingTable, TrouveAddress, TrouveType


"""

# A routing entry that adds "_dev" to the database name.
DEV_SUFFIX_ENTRY = """
    class DevRouting(RoutingEntry):
        environment_name: str = "dev"

        def route(
            self, trouve_address: TrouveAddress, trouve_type: TrouveType
        ) -> TrouveAddress:
            return trouve_address.model_copy(update={
                "database_name": f"{trouve_address.database_name}_dev"
            })


    routing = RoutingTable(entries=[DevRouting()])
"""

# A routing entry that makes a database name with a dash, which Snowflake refuses.
DASH_ENTRY = """
    class DashRouting(RoutingEntry):
        environment_name: str = "dev"

        def route(
            self, trouve_address: TrouveAddress, trouve_type: TrouveType
        ) -> TrouveAddress:
            return TrouveAddress(
                database_name=f"{trouve_address.database_name}-dev",
                schema_name=trouve_address.schema_name,
                table_name=trouve_address.table_name,
            )


    routing = RoutingTable(entries=[DashRouting()])
"""

# A routing entry that sends each Trouve to one schema, thus two names collide.
COLLIDING_ENTRY = """
    class SharedRouting(RoutingEntry):
        environment_name: str = "dev"

        def route(
            self, trouve_address: TrouveAddress, trouve_type: TrouveType
        ) -> TrouveAddress:
            return trouve_address.model_copy(update={
                "database_name": "DEV", "schema_name": "shared"
            })


    routing = RoutingTable(entries=[SharedRouting()])
"""


def entry_for_environment(environment_name: str) -> str:
    """Give a passthrough routing entry that names one environment."""
    return f"""
    class NamedRouting(RoutingEntry):
        environment_name: str = "{environment_name}"

        def route(
            self, trouve_address: TrouveAddress, trouve_type: TrouveType
        ) -> TrouveAddress:
            return trouve_address


    routing = RoutingTable(entries=[NamedRouting()])
"""


@pytest.fixture
def project_with_trouves(tmp_path: Path) -> Path:
    """A project with one SOURCE Trouve and two TABLE Trouves.

    The two TABLE Trouves share the table name `revenue` below two schemas, thus
    an entry that collapses the schema makes a collision.
    """
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


def write_routing(project_dir: Path, body: str, prelude: bool = True) -> None:
    """Write a `__routing__.py` in the project."""
    content = textwrap.dedent(body)
    (project_dir / "__routing__.py").write_text(
        _PRELUDE + content if prelude else content
    )


def problem_addresses(report: ValidationReport) -> list[str]:
    """Give the Trouve of each address problem, in name order."""
    return sorted(problem.logical_address for problem in report.address_problems)


class TestAValidProject:
    """A project with no problem gives an empty report."""

    def test_no_routing_file_is_valid(self, project_with_trouves: Path):
        report = clair.validate(project_with_trouves)
        assert report.is_valid is True
        assert report.problem_count == 0
        assert report.routing_file is None

    def test_a_valid_entry_is_valid(self, project_with_trouves: Path):
        write_routing(project_with_trouves, DEV_SUFFIX_ENTRY)
        report = clair.validate(project_with_trouves)
        assert report.is_valid is True
        assert report.address_problems == []
        assert report.collisions == []
        assert report.text_references == []

    def test_the_report_names_the_environment_and_the_entry(
        self, project_with_trouves: Path
    ):
        write_routing(project_with_trouves, DEV_SUFFIX_ENTRY)
        report = clair.validate(project_with_trouves)
        assert report.env_name == "dev"
        # The description names the entry class, not the base class.
        assert "DevRouting" in report.routing_description

    def test_the_report_finds_the_routing_file(self, project_with_trouves: Path):
        write_routing(project_with_trouves, DEV_SUFFIX_ENTRY)
        report = clair.validate(project_with_trouves)
        assert report.routing_file == project_with_trouves / "__routing__.py"

    def test_every_trouve_routes_and_a_source_is_not_an_exception(
        self, project_with_trouves: Path
    ):
        assert clair.validate(project_with_trouves).routable_count == 3

    def test_an_empty_project_routes_nothing(self, tmp_path: Path):
        project_dir = tmp_path / "empty"
        project_dir.mkdir()
        report = clair.validate(project_dir)
        assert report.routable_count == 0
        assert report.is_valid is True


class TestAddressProblems:
    """An entry that makes an address that Snowflake refuses."""

    def test_an_invalid_identifier_is_a_problem(self, project_with_trouves: Path):
        write_routing(project_with_trouves, DASH_ENTRY)
        report = clair.validate(project_with_trouves)
        assert report.is_valid is False
        assert len(report.address_problems) == 3

    def test_the_report_names_every_bad_trouve_not_only_the_first(
        self, project_with_trouves: Path
    ):
        write_routing(project_with_trouves, DASH_ENTRY)
        report = clair.validate(project_with_trouves)
        assert problem_addresses(report) == [
            "analytics.finance.revenue",
            "analytics.reports.revenue",
            "source.raw.orders",
        ]

    def test_each_problem_gives_the_reason(self, project_with_trouves: Path):
        write_routing(project_with_trouves, DASH_ENTRY)
        report = clair.validate(project_with_trouves)
        assert all(problem.detail for problem in report.address_problems)

    def test_an_entry_that_raises_is_a_problem(
        self, project_with_trouves: Path, monkeypatch
    ):
        """An entry that reads an absent variable must not stop the report."""
        monkeypatch.delenv("CLAIR_USER", raising=False)
        write_routing(
            project_with_trouves,
            """
            class UserRouting(RoutingEntry):
                environment_name: str = "dev"

                def route(
                    self, trouve_address: TrouveAddress, trouve_type: TrouveType
                ) -> TrouveAddress:
                    user_name = os.environ["CLAIR_USER"]
                    return trouve_address.model_copy(update={
                        "database_name": f"{trouve_address.database_name}_{user_name}"
                    })


            routing = RoutingTable(entries=[UserRouting()])
            """,
        )
        report = clair.validate(project_with_trouves)
        assert report.is_valid is False
        assert len(report.address_problems) == 3

    def test_a_bad_address_holds_the_collision_test_back(
        self, project_with_trouves: Path
    ):
        """Clair cannot make the physical addresses, thus it reports no collision."""
        write_routing(project_with_trouves, DASH_ENTRY)
        assert clair.validate(project_with_trouves).collisions == []


class TestCollisions:
    """Two Trouves that write to one physical address."""

    def test_a_collision_makes_the_project_invalid(self, project_with_trouves: Path):
        write_routing(project_with_trouves, COLLIDING_ENTRY)
        report = clair.validate(project_with_trouves)
        assert report.is_valid is False
        assert len(report.collisions) == 1

    def test_the_collision_names_the_target_and_each_source(
        self, project_with_trouves: Path
    ):
        write_routing(project_with_trouves, COLLIDING_ENTRY)
        collision = clair.validate(project_with_trouves).collisions[0]
        assert collision.physical_address == "DEV.shared.revenue"
        assert collision.logical_addresses == [
            "analytics.finance.revenue",
            "analytics.reports.revenue",
        ]

    def test_a_trouve_that_goes_to_its_own_address_makes_no_collision(
        self, project_with_trouves: Path
    ):
        write_routing(project_with_trouves, DEV_SUFFIX_ENTRY)
        assert clair.validate(project_with_trouves).collisions == []


class TestTextReferences:
    """An address that an author writes as text makes no DAG edge."""

    def test_an_address_in_the_sql_is_a_problem(self, project_with_trouves: Path):
        (project_with_trouves / "analytics" / "finance" / "revenue.py").write_text(
            "from clair import Trouve, TrouveType\n\n"
            "trouve = Trouve(\n"
            "    type=TrouveType.TABLE,\n"
            '    sql="SELECT * FROM source.raw.orders",\n'
            ")\n"
        )
        report = clair.validate(project_with_trouves)
        assert report.is_valid is False
        assert len(report.text_references) == 1

        reference = report.text_references[0]
        assert reference.logical_address == "analytics.finance.revenue"
        assert reference.text_address == "source.raw.orders"
        assert reference.location == TextReferenceLocation.SQL


class TestProblemCount:
    """The count adds the problems of each kind."""

    def test_a_valid_project_counts_zero(self, project_with_trouves: Path):
        assert clair.validate(project_with_trouves).problem_count == 0

    def test_the_count_adds_each_kind(self, project_with_trouves: Path):
        write_routing(project_with_trouves, COLLIDING_ENTRY)
        (project_with_trouves / "analytics" / "reports" / "revenue.py").write_text(
            "from clair import Trouve, TrouveType\n\n"
            "trouve = Trouve(\n"
            "    type=TrouveType.TABLE,\n"
            '    sql="SELECT * FROM source.raw.orders",\n'
            ")\n"
        )
        report = clair.validate(project_with_trouves)
        assert report.problem_count == len(report.collisions) + len(
            report.text_references
        )
        assert report.problem_count == 2


class TestTheEnvironment:
    """The environment selects the routing entry."""

    def test_the_default_environment_is_dev(self, project_with_trouves: Path):
        assert clair.validate(project_with_trouves).env_name == "dev"

    def test_the_argument_selects_the_environment(self, project_with_trouves: Path):
        write_routing(project_with_trouves, entry_for_environment("prod"))
        report = clair.validate(project_with_trouves, env="prod")
        assert report.env_name == "prod"
        assert report.unnamed_environment_warning is None

    def test_the_environment_variable_selects_the_environment(
        self, project_with_trouves: Path, monkeypatch
    ):
        monkeypatch.setenv("CLAIR_ENV", "staging")
        write_routing(project_with_trouves, entry_for_environment("staging"))
        assert clair.validate(project_with_trouves).env_name == "staging"

    def test_the_argument_wins_against_the_environment_variable(
        self, project_with_trouves: Path, monkeypatch
    ):
        monkeypatch.setenv("CLAIR_ENV", "staging")
        write_routing(project_with_trouves, entry_for_environment("prod"))
        assert clair.validate(project_with_trouves, env="prod").env_name == "prod"

    def test_a_file_that_omits_the_environment_warns(self, project_with_trouves: Path):
        """An absent entry is almost always a typo, and clair says so."""
        write_routing(project_with_trouves, DEV_SUFFIX_ENTRY)
        report = clair.validate(project_with_trouves, env="typo")
        assert report.unnamed_environment_warning is not None
        assert "typo" in report.unnamed_environment_warning

    def test_the_warning_is_not_a_problem(self, project_with_trouves: Path):
        """Passthrough routing is valid, thus the warning changes no exit code."""
        write_routing(project_with_trouves, DEV_SUFFIX_ENTRY)
        report = clair.validate(project_with_trouves, env="typo")
        assert report.is_valid is True

    def test_no_routing_file_gives_no_warning(self, project_with_trouves: Path):
        assert clair.validate(project_with_trouves).unnamed_environment_warning is None


class TestBadInput:
    """A fault that stops the report raises, and it gives no report."""

    def test_a_broken_routing_file_raises(self, project_with_trouves: Path):
        write_routing(project_with_trouves, "routing = [\n", prelude=False)
        with pytest.raises(ClairError):
            clair.validate(project_with_trouves)

    def test_a_duplicate_environment_name_raises(self, project_with_trouves: Path):
        write_routing(
            project_with_trouves,
            """
            class DevRouting(RoutingEntry):
                environment_name: str = "dev"

                def route(
                    self, trouve_address: TrouveAddress, trouve_type: TrouveType
                ) -> TrouveAddress:
                    return trouve_address


            routing = RoutingTable(entries=[DevRouting(), DevRouting()])
            """,
        )
        with pytest.raises(ClairError):
            clair.validate(project_with_trouves)

    def test_a_bad_directory_name_raises(self, tmp_path: Path):
        """A directory that Snowflake cannot use as a name stops validate."""
        project_dir = tmp_path / "proj"
        (project_dir / "my-db" / "finance").mkdir(parents=True)
        (project_dir / "my-db" / "finance" / "revenue.py").write_text(
            "from clair import Trouve, TrouveType\n\n"
            'trouve = Trouve(type=TrouveType.TABLE, sql="SELECT 1 AS x")\n'
        )
        with pytest.raises(ClairError):
            clair.validate(project_dir)


class TestTheCommand:
    """The exit code of `clair validate`. The report tests cover the semantics."""

    def run_command(self, project_dir: Path, *arguments: str):
        return CliRunner().invoke(
            cli, ["validate", "--project", str(project_dir), *arguments]
        )

    def test_a_valid_project_exits_zero(self, project_with_trouves: Path):
        assert self.run_command(project_with_trouves).exit_code == 0

    @pytest.mark.parametrize("entry", [DASH_ENTRY, COLLIDING_ENTRY])
    def test_a_problem_exits_one(self, project_with_trouves: Path, entry: str):
        write_routing(project_with_trouves, entry)
        assert self.run_command(project_with_trouves).exit_code == 1

    def test_a_broken_routing_file_exits_one(self, project_with_trouves: Path):
        write_routing(project_with_trouves, "routing = [\n", prelude=False)
        assert self.run_command(project_with_trouves).exit_code == 1

    def test_the_command_shows_the_report(self, project_with_trouves: Path):
        # Make the report first. The CLI runner closes the stream that it
        # captured, and a later call of the API then writes to a closed file.
        expected = clair.validate(project_with_trouves).render()
        result = self.run_command(project_with_trouves)
        assert expected in result.output


class TestTheReportText:
    """The text of the report. The data tests above cover the semantics."""

    def test_a_valid_report_says_that_each_address_is_valid(
        self, project_with_trouves: Path
    ):
        assert "✓" in clair.validate(project_with_trouves).render()

    def test_a_report_with_one_problem_uses_the_singular_word(
        self, project_with_trouves: Path
    ):
        write_routing(project_with_trouves, COLLIDING_ENTRY)
        assert "1 problem found." in clair.validate(project_with_trouves).render()

    def test_a_report_with_more_problems_uses_the_plural_word(
        self, project_with_trouves: Path
    ):
        write_routing(project_with_trouves, DASH_ENTRY)
        assert "3 problems found." in clair.validate(project_with_trouves).render()
