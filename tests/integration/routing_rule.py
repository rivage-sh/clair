"""The rule that makes one physical table name for the pull request tests.

The routing entry of a project copy and the assertion of a test must make the
same name. This module holds the rule one time, and both read it. A copy of the
rule becomes wrong when one caller changes.

The prefix isolates one test class. The tests of a pull request share one
schema, and two classes can build the same example project. Without the prefix
they write the same table, and a parallel run makes them race.
`tests/integration/conftest.py` sets the variable for each class.
"""

from __future__ import annotations

import os

TABLE_PREFIX_VARIABLE = "CLAIR_PR_TESTING_TABLE_PREFIX"


def table_prefix() -> str:
    """Give the prefix of the test class that runs now, or an empty text."""
    return os.environ.get(TABLE_PREFIX_VARIABLE, "")


def physical_table_name(database_name: str, schema_name: str, table_name: str) -> str:
    """Make the one table name that holds each logical part and the prefix."""
    routed_name = f"{database_name}__{schema_name}__{table_name}"
    prefix = table_prefix()
    return f"{prefix}__{routed_name}" if prefix else routed_name
