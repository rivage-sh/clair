"""Prove that the query tag and the statement timeout of a Trouve reach Snowflake.

One Trouve carries a label, and its statements must show the label and the
Trouve in QUERY_HISTORY. A second Trouve waits longer than its timeout, and
Snowflake must cancel it.
"""

from __future__ import annotations

import json

import pytest

import clair
from clair.adapters.snowflake import SnowflakeAdapter
from clair.core.runner import RunStatus, RunSummary
from clair.environments.environments import Environment
from tests.integration.config import DATABASE_NAME as WAREHOUSE_DATABASE_NAME
from tests.integration.projects import write_refined_project
from tests.integration.warehouse import query_rows

pytestmark = pytest.mark.integration

DATABASE_NAME = "session_config"
TAGGED = f"{DATABASE_NAME}.refined.tagged"
SLOW = f"{DATABASE_NAME}.refined.slow"


class TestTheSession:
    @pytest.fixture(scope="class")
    def summary(
        self,
        environment: Environment,
        tmp_path_factory: pytest.TempPathFactory,
        workspace_prefix: str,
    ) -> RunSummary:
        project_path = write_refined_project(
            tmp_path_factory.mktemp(DATABASE_NAME),
            DATABASE_NAME,
            {
                "tagged": (
                    "from clair import Table\n"
                    'trouve = Table(sql="SELECT 1 AS id", query_tags={"team": "finance"})\n'
                ),
                "slow": (
                    "from clair import Table\n"
                    "trouve = Table(sql=\"SELECT SYSTEM$WAIT(5) AS waited\", statement_timeout_seconds=1)\n"
                ),
            },
        )
        return clair.run(project_path, env=environment, test=False)

    def test_the_query_history_shows_the_label_and_the_trouve(
        self, summary: RunSummary, adapter: SnowflakeAdapter
    ) -> None:
        result = summary.result(TAGGED)
        assert result.status == RunStatus.SUCCESS, result.error
        query_id = next(statement.query_id for statement in result.statements if statement.query_id)

        ((query_tag,),) = query_rows(
            adapter,
            f"SELECT query_tag FROM TABLE({WAREHOUSE_DATABASE_NAME}.INFORMATION_SCHEMA.QUERY_HISTORY("
            "RESULT_LIMIT => 10000)) "
            f"WHERE query_id = '{query_id}'",
        )
        tag = json.loads(str(query_tag))
        assert (tag["team"], tag["trouve"], tag["run_id"]) == ("finance", TAGGED, summary.run_id)

    def test_a_statement_longer_than_the_timeout_fails(self, summary: RunSummary) -> None:
        result = summary.result(SLOW)
        assert result.status == RunStatus.FAILURE
        assert "timeout" in result.error.lower()
