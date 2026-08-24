"""Examine the pipeline project without a Snowflake connection.

`clair validate` and `clair dag` need no credentials. These tests therefore run
on each pull request, and a pull request from a fork runs them too. They catch a
fault in tests/integration/pipeline_project before a Snowflake job starts.
"""

from __future__ import annotations

import os

from tests.integration.conftest import PIPELINE_PROJECT_PATH, run_clair

EXAMPLE_SCHEMA_PREFIX = "PR_42"


def offline_environment() -> dict[str, str]:
    """Give the environment of a command that opens no connection."""
    environment = dict(os.environ)
    environment["CLAIR_ENV"] = "ci"
    environment["CLAIR_CI_SCHEMA_PREFIX"] = EXAMPLE_SCHEMA_PREFIX
    return environment


def test_validate_finds_no_problem() -> None:
    """Each routed name is valid, and no two Trouves go to one target."""
    completed = run_clair(
        ["validate", "--project", str(PIPELINE_PROJECT_PATH)], offline_environment()
    )
    assert "No collisions" in completed.stdout
    assert "Trouves to route: 6" in completed.stdout


def test_the_dag_has_the_expected_shape() -> None:
    """The project holds 6 models and 2 sources."""
    completed = run_clair(
        ["dag", "--project", str(PIPELINE_PROJECT_PATH)], offline_environment()
    )
    assert "6 models, 2 sources" in completed.stdout
    assert "clair_ci.derived.user_purchase_summary  [VIEW]" in completed.stdout
