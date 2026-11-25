"""Prove what the built-in data quality tests accept in Snowflake."""

from __future__ import annotations

import pytest

import clair
from clair.environments.environments import Environment
from tests.integration.projects import write_refined_project

pytestmark = pytest.mark.integration

DATABASE_NAME = "data_quality_tests"


class TestUniqueAndNull:
    def test_two_nulls_are_no_duplicate(
        self,
        environment: Environment,
        tmp_path_factory: pytest.TempPathFactory,
        workspace_prefix: str,
    ) -> None:
        """A NULL is not a value, thus a nullable key with two NULLs is unique, as in dbt."""
        project_path = write_refined_project(
            tmp_path_factory.mktemp(DATABASE_NAME),
            DATABASE_NAME,
            {
                "optional_keys": (
                    "from clair import Table, TestUnique\n"
                    "trouve = Table(\n"
                    '    sql="SELECT NULL AS external_id UNION ALL SELECT NULL UNION ALL SELECT 1",\n'
                    '    tests=[TestUnique(column="external_id")],\n'
                    ")\n"
                ),
            },
        )

        summary = clair.run(project_path, env=environment)

        assert summary.failed == []
        assert [test_result.passed for test_result in summary.test_results] == [True]
