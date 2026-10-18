"""The session configuration: a Trouve can set its warehouse, role, labels and timeout.

The database, the schema and the Trouve each set the same fields, and the lower
level wins. Each statement carries a JSON query tag, thus the query history
shows the run, the environment and the Trouve with no configuration.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from pydantic import ValidationError

from clair.core.discovery import discover_project
from clair.trouves.config import QUERY_TAG_KEYS, ResolvedConfig, SessionConfig
from clair.trouves.run_config import RunMode
from clair.trouves.trouve import Table
from tests.helpers import (
    TEST_ADDRESS,
    TEST_RUN_ID,
    RecordingAdapter,
    run_one_trouve,
    snowflake_adapter_that_answers,
    write_project_marker,
)


def write_layered_project(root: Path, database_text: str, schema_text: str, trouve_text: str) -> Path:
    """Write one Trouve with a database config and a schema config above it."""
    write_project_marker(root)
    schema_directory = root / "mydb" / "myschema"
    schema_directory.mkdir(parents=True)
    (root / "mydb" / "__database_config__.py").write_text(
        f"from clair import DatabaseDefaults\ndefaults = DatabaseDefaults({database_text})\n"
    )
    (schema_directory / "__schema_config__.py").write_text(
        f"from clair import SchemaDefaults\ndefaults = SchemaDefaults({schema_text})\n"
    )
    (schema_directory / "orders.py").write_text(
        f'from clair import Table\ntrouve = Table(sql="select 1 as n", {trouve_text})\n'
    )
    return root


def resolved_config_of(root: Path) -> ResolvedConfig:
    (trouve,) = discover_project(root)
    assert trouve.compiled is not None
    return trouve.compiled.config


class TestTheLevels:
    @pytest.mark.parametrize(
        ("database_text", "schema_text", "trouve_text", "expected_warehouse"),
        [
            ('warehouse="database_wh"', "", "", "database_wh"),
            ('warehouse="database_wh"', 'warehouse="schema_wh"', "", "schema_wh"),
            ('warehouse="database_wh"', 'warehouse="schema_wh"', 'warehouse="trouve_wh"', "trouve_wh"),
            ('warehouse="database_wh"', "", 'warehouse="  "', "database_wh"),
        ],
    )
    def test_the_lowest_level_that_sets_the_warehouse_wins(
        self, tmp_path, database_text, schema_text, trouve_text, expected_warehouse
    ):
        root = write_layered_project(tmp_path, database_text, schema_text, trouve_text)
        assert resolved_config_of(root).warehouse == expected_warehouse

    def test_the_labels_merge_by_key(self, tmp_path):
        root = write_layered_project(
            tmp_path,
            'query_tags={"team": "data", "cost_center": "42"}',
            "",
            'query_tags={"team": "finance"}',
        )
        assert resolved_config_of(root).query_tags == {"team": "finance", "cost_center": "42"}

    def test_a_lower_level_timeout_replaces_the_higher_one(self, tmp_path):
        root = write_layered_project(
            tmp_path, "statement_timeout_seconds=3600", "", "statement_timeout_seconds=60"
        )
        assert resolved_config_of(root).statement_timeout_seconds == 60


class TestTheFields:
    @pytest.mark.parametrize("reserved_key", sorted(QUERY_TAG_KEYS))
    def test_a_label_cannot_take_a_key_of_clair(self, reserved_key):
        with pytest.raises(ValidationError, match="Give each label another key"):
            SessionConfig(query_tags={reserved_key: "x"})

    @pytest.mark.parametrize("timeout", [0, -1])
    def test_a_timeout_must_be_positive(self, timeout):
        with pytest.raises(ValidationError):
            Table(sql="select 1", statement_timeout_seconds=timeout)

    def test_the_query_tag_is_json_with_the_keys_of_clair_and_the_labels(self):
        config = ResolvedConfig(query_tags={"team": "finance"})
        tag = json.loads(config.query_tag("run_1", "prod", "mydb.myschema.orders"))

        assert set(tag) == QUERY_TAG_KEYS | {"team"}
        assert (tag["app"], tag["run_id"], tag["environment"], tag["trouve"], tag["team"]) == (
            "clair", "run_1", "prod", "mydb.myschema.orders", "finance",
        )


class TestTheRun:
    def test_each_node_sets_its_tag_and_timeout_before_its_statements(self):
        adapter = RecordingAdapter()
        run_one_trouve(Table(sql="select 1", statement_timeout_seconds=90), adapter, run_mode=RunMode.FULL_REFRESH)

        ((query_tag, timeout),) = adapter.session_parameters
        assert json.loads(query_tag)["trouve"] == TEST_ADDRESS
        assert json.loads(query_tag)["run_id"] == TEST_RUN_ID
        assert timeout == 90

    def test_a_trouve_warehouse_reaches_the_session(self):
        adapter = RecordingAdapter()
        trouve = Table(sql="select 1", warehouse="big_wh")
        run_one_trouve(trouve, adapter)

        assert [context["warehouse"] for context in adapter.contexts] == ["big_wh"]


class TestSnowflakeSessionParameters:
    def test_a_timeout_sets_the_parameter(self):
        adapter, cursor = snowflake_adapter_that_answers([])
        adapter.set_session_parameters('{"app": "clair"}', 90)
        assert cursor.executed == [
            """ALTER SESSION SET QUERY_TAG = '{"app": "clair"}'""",
            "ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = 90",
        ]

    def test_no_timeout_unsets_the_timeout_of_the_node_before(self):
        adapter, cursor = snowflake_adapter_that_answers([])
        adapter.set_session_parameters("{}", None)
        assert cursor.executed[-1] == "ALTER SESSION UNSET STATEMENT_TIMEOUT_IN_SECONDS"

    def test_a_quote_in_the_tag_stays_inside_the_string(self):
        adapter, cursor = snowflake_adapter_that_answers([])
        adapter.set_session_parameters('{"team": "o\'brien"}', None)
        assert cursor.executed[0] == """ALTER SESSION SET QUERY_TAG = '{"team": "o\\'brien"}'"""
