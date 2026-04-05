"""Clair -- Python-native data transformation framework for Snowflake.

Public API exports. Users import from here in their Trouve files:

    from clair import Trouve, TrouveType, Column, ColumnType

Runtime context
---------------
``clair.env`` is set to the active :class:`~clair.environments.environments.Environment`
before Trouve modules are loaded during discovery. Import it to implement
feature flags based on the active environment::

    import clair

    trouve = Trouve(
        sql=f"SELECT * FROM {upstream} {'WHERE is_beta = 1' if clair.env.role == 'DEV' else ''}"
    )

``clair.run_mode`` is set to the active :class:`~clair.trouves.run_config.RunMode`
before Trouve modules are loaded during discovery. Use it to make SQL conditional
on run mode (analogous to dbt's ``is_incremental()``)::

    import clair
    from clair import RunMode

    trouve = Trouve(
        sql=f\"\"\"
            SELECT * FROM {upstream}
            {'WHERE created_at > dateadd(\\'day\\', -3, current_timestamp())' if clair.run_mode == RunMode.INCREMENTAL else ''}
        \"\"\"
    )

When ``clair.run_mode`` is ``None`` (e.g. during ``clair dag`` or ``clair docs``),
the expression ``clair.run_mode == RunMode.INCREMENTAL`` evaluates to ``False``,
so no WHERE clause is applied — the safe default for non-run commands.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from clair.environments.environments import Environment

from clair.trouves.column import Column, ColumnType
from clair.trouves.config import DatabaseDefaults, SchemaDefaults
from clair.trouves.run_config import (
    IncrementalMode,
    UpsertConfig,
    RunConfig,
    RunMode,
    SOURCE,
    TARGET,
)
from clair.trouves.test import (
    AnyTest,
    Test,
    TestNotNull,
    TestRowCount,
    TestSql,
    TestUnique,
    TestUniqueColumns,
    THIS,
)
from clair.trouves.trouve import Trouve, TrouveType

__version__ = "0.1.0"

# Set by discover_project() before Trouve modules are loaded.
# None when running outside of a clair discovery run.
env: Environment | None = None

# Set by discover_project() before Trouve modules are loaded.
# None when running outside of a clair discovery run.
run_mode: RunMode | None = None

__all__ = [
    "AnyTest",
    "Column",
    "ColumnType",
    "DatabaseDefaults",
    "IncrementalMode",
    "UpsertConfig",
    "RunConfig",
    "RunMode",
    "SOURCE",
    "SchemaDefaults",
    "TARGET",
    "Test",
    "TestNotNull",
    "TestRowCount",
    "TestSql",
    "TestUnique",
    "TestUniqueColumns",
    "THIS",
    "Trouve",
    "TrouveType",
]
