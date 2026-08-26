"""The tests of find_text_references, the lint for an address that is text."""

from __future__ import annotations

import textwrap
from pathlib import Path

from clair.core.discovery import discover_project
from clair.core.text_references import TextReferenceLocation, find_text_references


def _write_source(root: Path) -> None:
    (root / "mydb" / "source").mkdir(parents=True, exist_ok=True)
    (root / "mydb" / "source" / "events.py").write_text(textwrap.dedent("""\
        from clair import Trouve, TrouveType
        trouve = Trouve(type=TrouveType.SOURCE)
    """))


def _write_refined(root: Path, sql_literal: str) -> None:
    """Write mydb/refined/report.py with the SQL that the test gives."""
    (root / "mydb" / "refined").mkdir(parents=True, exist_ok=True)
    (root / "mydb" / "refined" / "report.py").write_text(textwrap.dedent(f"""\
        from mydb.source.events import trouve as source_events
        from clair import Trouve, TrouveType
        trouve = Trouve(type=TrouveType.TABLE, sql={sql_literal})
    """))


class TestFindTextReferences:
    def test_an_address_as_text_is_a_fault(self, tmp_path: Path):
        """The SQL names a Trouve of this project as text, thus clair reports it."""
        _write_source(tmp_path)
        _write_refined(tmp_path, '"SELECT * FROM mydb.source.events"')

        references = find_text_references(discover_project(tmp_path))

        assert len(references) == 1
        assert references[0].logical_address == "mydb.refined.report"
        assert references[0].text_address == "mydb.source.events"
        assert references[0].location == TextReferenceLocation.SQL

    def test_an_f_string_reference_is_correct(self, tmp_path: Path):
        """The author imports the Trouve, thus the SQL holds a token."""
        _write_source(tmp_path)
        _write_refined(tmp_path, 'f"SELECT * FROM {source_events}"')

        assert find_text_references(discover_project(tmp_path)) == []

    def test_an_address_in_a_line_comment_is_correct(self, tmp_path: Path):
        """A comment is not a table name, thus the syntax tree does not hold it."""
        _write_source(tmp_path)
        _write_refined(
            tmp_path,
            'f"""\\n-- The old name was mydb.source.events.\\nSELECT * FROM {source_events}"""',
        )

        assert find_text_references(discover_project(tmp_path)) == []

    def test_an_address_in_a_block_comment_is_correct(self, tmp_path: Path):
        _write_source(tmp_path)
        _write_refined(
            tmp_path,
            'f"""SELECT /* mydb.source.events */ * FROM {source_events}"""',
        )

        assert find_text_references(discover_project(tmp_path)) == []

    def test_an_address_in_a_string_literal_is_correct(self, tmp_path: Path):
        """A string literal holds data, and not a table name."""
        _write_source(tmp_path)
        _write_refined(
            tmp_path,
            "f\"SELECT 'mydb.source.events' AS name FROM {source_events}\"",
        )

        assert find_text_references(discover_project(tmp_path)) == []

    def test_a_table_outside_the_project_is_correct(self, tmp_path: Path):
        """Clair does not hold this table, thus the name is correct SQL."""
        _write_source(tmp_path)
        _write_refined(tmp_path, '"SELECT * FROM other.database.table_name"')

        assert find_text_references(discover_project(tmp_path)) == []

    def test_a_cte_name_is_correct(self, tmp_path: Path):
        """A CTE name has one part, thus it is never a logical address."""
        _write_source(tmp_path)
        _write_refined(
            tmp_path,
            'f"WITH daily AS (SELECT * FROM {source_events}) SELECT * FROM daily"',
        )

        assert find_text_references(discover_project(tmp_path)) == []

    def test_sql_that_the_parser_cannot_read_gives_no_fault(self, tmp_path: Path):
        """Snowflake owns the SQL syntax. Clair reports no parse fault."""
        _write_source(tmp_path)
        _write_refined(tmp_path, '"SELECT * FROM WHERE ORDER BY )("')

        assert find_text_references(discover_project(tmp_path)) == []

    def test_the_name_matches_without_case_sensitivity(self, tmp_path: Path):
        """Snowflake reads an unquoted name without case sensitivity."""
        _write_source(tmp_path)
        _write_refined(tmp_path, '"SELECT * FROM MYDB.SOURCE.EVENTS"')

        references = find_text_references(discover_project(tmp_path))

        assert len(references) == 1
        # The report gives the form that the project uses.
        assert references[0].text_address == "mydb.source.events"

    def test_a_test_sql_gets_the_same_lint(self, tmp_path: Path):
        """A TestSql holds addresses too, thus it gets the same rule."""
        _write_source(tmp_path)
        (tmp_path / "mydb" / "refined").mkdir(parents=True, exist_ok=True)
        (tmp_path / "mydb" / "refined" / "report.py").write_text(textwrap.dedent("""\
            from mydb.source.events import trouve as source_events
            from clair import Trouve, TrouveType, TestSql, THIS
            trouve = Trouve(
                type=TrouveType.TABLE,
                sql=f"SELECT * FROM {source_events}",
                tests=[TestSql(sql=f"SELECT * FROM {THIS} WHERE id NOT IN (SELECT id FROM mydb.source.events)")],
            )
        """))

        references = find_text_references(discover_project(tmp_path))

        assert len(references) == 1
        assert references[0].location == TextReferenceLocation.TEST_SQL
        assert references[0].text_address == "mydb.source.events"

    def test_a_pandas_trouve_has_no_sql_to_read(self, tmp_path: Path):
        """A pandas Trouve names its inputs in a list, thus this lint skips it."""
        _write_source(tmp_path)
        (tmp_path / "mydb" / "derived").mkdir(parents=True, exist_ok=True)
        (tmp_path / "mydb" / "derived" / "summary.py").write_text(textwrap.dedent("""\
            import pandas as pd
            from clair import PandasTrouve
            from mydb.source.events import trouve as source_events

            def summarize(events: pd.DataFrame) -> pd.DataFrame:
                return events

            trouve = PandasTrouve(transform=summarize, inputs=[source_events])
        """))

        assert find_text_references(discover_project(tmp_path)) == []
