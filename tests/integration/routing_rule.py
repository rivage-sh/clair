"""The rule that makes one physical table name for the pull request tests.

The routing entry of a project copy and the assertion of a test must make the
same name. This module holds the rule one time, and both read it. A copy of the
rule becomes wrong when one caller changes.

The prefix isolates one test class. The tests of a pull request share one
schema, and two classes can build the same example project. Without the prefix
they write the same table, and a parallel run makes them race.
`tests/integration/conftest.py` sets the variable for each class.

An environment variable carries the prefix, because clair makes the routing
entry itself. The `__routing__.py` of a project copy takes no argument from a
test, thus the process holds the value.
"""

from __future__ import annotations

import os

TABLE_PREFIX_VARIABLE = "CLAIR_PR_TESTING_TABLE_PREFIX"


class MissingTablePrefixError(RuntimeError):
    """The process holds no table prefix, thus a name would collide in silence."""


def table_prefix() -> str:
    """Give the prefix of the test class that runs now.

    The function raises when the variable is absent. An empty prefix gives the
    name of a class to each other class, thus two classes write one table and
    nothing tells you. The `workspace_prefix` fixture is autouse, so the
    variable is present for each test. A caller outside pytest must set it.
    """
    prefix = os.environ.get(TABLE_PREFIX_VARIABLE)
    if not prefix:
        raise MissingTablePrefixError(
            f"{TABLE_PREFIX_VARIABLE} is empty or absent. The workspace_prefix "
            "fixture in tests/integration/conftest.py sets it for each test "
            "class. Set it yourself to make a name outside pytest."
        )
    return prefix


def _name_part(name: str) -> str:
    """Keep the characters of *name* that a Snowflake identifier accepts."""
    return "".join(
        character for character in name.lower() if character.isalnum() or character == "_"
    )


def workspace_prefix_of(module_name: str, class_name: str | None) -> str:
    """Make the prefix of one test class from its module and its class.

    The module is part of the prefix, and the class is not the prefix alone. Two
    modules can hold one class name — `TestASuccessfulRun` is a name that
    repeats — and two classes with one prefix write one table, in silence. The
    module and the class together are unique, because two modules of one package
    cannot share a name.

    A test outside a class takes the module name alone.
    """
    parts = [_name_part(module_name)]
    if class_name is not None:
        parts.append(_name_part(class_name))
    return "__".join(parts)


def make_table_name(database_name: str, schema_name: str, table_name: str) -> str:
    """Make the one table name that holds each logical part and the prefix."""
    return f"{table_prefix()}__{database_name}__{schema_name}__{table_name}"
