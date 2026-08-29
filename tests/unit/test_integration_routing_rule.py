"""The name rule of the integration tests, without Snowflake.

`tests/integration/routing_rule.py` gives one physical table name to a project
copy and to the assertion of a test. The rule decides if two test classes share
a table, thus this unit test holds it. The integration job needs a warehouse,
and it cannot find a fault here.
"""

from __future__ import annotations

import pytest

from tests.integration.routing_rule import (
    TABLE_PREFIX_VARIABLE,
    MissingTablePrefixError,
    make_table_name,
    workspace_prefix_of,
)


@pytest.mark.parametrize(
    ("module_name", "class_name", "expected_prefix"),
    [
        ("test_examples", None, "test_examples"),
        ("test_staging", "TestASuccessfulRun", "test_staging__testasuccessfulrun"),
        ("test_seed", "TestASeedWithNoRow", "test_seed__testaseedwithnorow"),
        ("test_api_commands", "TestExclude", "test_api_commands__testexclude"),
    ],
)
def test_the_prefix_holds_the_module_and_the_class(
    module_name: str, class_name: str | None, expected_prefix: str
) -> None:
    assert workspace_prefix_of(module_name, class_name) == expected_prefix


def test_one_class_name_in_two_modules_gives_two_prefixes() -> None:
    """This is the collision that the module name removes."""
    first = workspace_prefix_of("test_staging", "TestASuccessfulRun")
    second = workspace_prefix_of("test_seed", "TestASuccessfulRun")
    assert first != second


def test_a_prefix_holds_no_character_that_snowflake_refuses() -> None:
    assert workspace_prefix_of("test-examples.v2", "Test Exclude!") == (
        "testexamplesv2__testexclude"
    )


def test_the_name_holds_the_prefix_and_each_logical_part(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(TABLE_PREFIX_VARIABLE, "test_examples")
    assert make_table_name("example_1_database", "refined", "events") == (
        "test_examples__example_1_database__refined__events"
    )


@pytest.mark.parametrize("prefix", ["", None])
def test_an_absent_prefix_raises_and_makes_no_name(
    monkeypatch: pytest.MonkeyPatch, prefix: str | None
) -> None:
    """An empty prefix gives one name to two classes, and they write one table."""
    if prefix is None:
        monkeypatch.delenv(TABLE_PREFIX_VARIABLE, raising=False)
    else:
        monkeypatch.setenv(TABLE_PREFIX_VARIABLE, prefix)

    with pytest.raises(MissingTablePrefixError):
        make_table_name("example_1_database", "refined", "events")
