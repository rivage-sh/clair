"""Clair -- Python-native data transformation framework for Snowflake.

Public API exports. Users import from here in their Trouve files:

    from clair import Trouve, TrouveType, Column, ColumnType

Operations
----------
The package also holds one function for each operation of the CLI. Each one
gives a result object with the complete data of the operation::

    import clair

    summary = clair.run("~/projects/analytics")
    print(summary.succeeded_count, summary.failed_count)

``clair.run()``, ``clair.compile()``, ``clair.test()``, ``clair.docs()`` and
``clair.catalog()`` come from :mod:`clair.api`.

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

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from clair.environments.environments import Environment

from clair.environments.routing import RoutingEntry, RoutingTable
from clair.trouves.address import TrouveAddress
from clair.trouves.column import Column, ColumnType
from clair.trouves.config import DatabaseDefaults, SchemaDefaults
from clair.trouves.pandas_trouve import PandasTrouve
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
from clair.trouves.trouve import Trouve, TrouveAbc, TrouveType

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
    "PandasTrouve",
    "RoutingEntry",
    "RoutingTable",
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
    "TrouveAbc",
    "TrouveAddress",
    "TrouveType",
    "UpsertConfig",
    "catalog",
    "compile",
    "docs",
    "run",
    "test",
]


# The operations of the Python API: clair.run(), clair.compile(), clair.test(),
# clair.docs() and clair.catalog(). The import is late, because clair.api
# imports the packages that read a project, and each of those packages imports
# this module.
_API_NAMES = frozenset({"catalog", "compile", "docs", "run", "test"})


def __getattr__(name: str) -> Any:
    if name in _API_NAMES:
        from clair import api

        return getattr(api, name)
    raise AttributeError(f"module 'clair' has no attribute '{name}'")
