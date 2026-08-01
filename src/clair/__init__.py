"""Clair -- Python-native data transformation framework for Snowflake.

Public API exports. Users import from here in their Trouve files:

    from clair import Trouve, TrouveType, Column, ColumnType

Runtime context
---------------
Discovery sets ``clair.env`` to the active
:class:`~clair.environments.environments.Environment` before it loads the Trouve
modules. Import it to make feature flags that obey the active environment::

    import clair

    trouve = Trouve(
        sql=f"SELECT * FROM {upstream} {'WHERE is_beta = 1' if clair.env.role == 'DEV' else ''}"
    )

Discovery also sets ``clair.run_mode`` to the active
:class:`~clair.trouves.run_config.RunMode` before it loads the Trouve modules. Use
it to make the SQL obey the run mode (the equivalent of dbt's ``is_incremental()``)::

    import clair
    from clair import RunMode

    trouve = Trouve(
        sql=f\"\"\"
            SELECT * FROM {upstream}
            {'WHERE created_at > dateadd(\\'day\\', -3, current_timestamp())' if clair.run_mode == RunMode.INCREMENTAL else ''}
        \"\"\"
    )

When ``clair.run_mode`` is ``None`` (for example, in ``clair dag`` or ``clair
docs``), the expression ``clair.run_mode == RunMode.INCREMENTAL`` gives ``False``.
Thus clair adds no WHERE clause — the safe default for commands that do not run
the project.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from clair.environments.environments import Environment

from clair.trouves.column import Column, ColumnType
from clair.trouves.config import DatabaseDefaults, SchemaDefaults
from clair.trouves.run_config import (
    SOURCE,
    TARGET,
    IncrementalMode,
    RunConfig,
    RunMode,
    UpsertConfig,
)
from clair.trouves.test import (
    THIS,
    AnyTest,
    Test,
    TestNotNull,
    TestRowCount,
    TestSql,
    TestUnique,
    TestUniqueColumns,
)
from clair.trouves.trouve import Trouve, TrouveType

__version__ = "0.1.0"

# discover_project() sets this before it loads the Trouve modules.
# It stays None outside of a clair discovery run.
env: Environment | None = None

# discover_project() sets this before it loads the Trouve modules.
# It stays None outside of a clair discovery run.
run_mode: RunMode | None = None

__all__ = [
    "SOURCE",
    "TARGET",
    "THIS",
    "AnyTest",
    "Column",
    "ColumnType",
    "DatabaseDefaults",
    "IncrementalMode",
    "RunConfig",
    "RunMode",
    "SchemaDefaults",
    "Test",
    "TestNotNull",
    "TestRowCount",
    "TestSql",
    "TestUnique",
    "TestUniqueColumns",
    "Trouve",
    "TrouveType",
    "UpsertConfig",
]
