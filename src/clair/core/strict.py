"""Strict mode -- materialize into a run-scoped staging object, test, then promote.

A table can only be tested once it has been materialized, so a plain run leaves a
window where the production object holds untested data. Strict mode closes that
window:

1. Materialize the Trouve into ``<table>__clair_strict_<run_id>``, a sibling object
   in the same schema. For incremental Trouves the current target is first
   zero-copy cloned into that name so the incremental statements have a base to
   apply to.
2. Run the Trouve's data quality tests against the staging object.
3. On pass, promote the staging object into the real name. For tables this is
   ``ALTER TABLE ... SWAP WITH ...``, a metadata-only operation whose cost does
   not scale with table size.
4. On failure, drop the staging object. The production object is never touched.

Downstream Trouves are unaffected: promotion happens immediately after each node's
tests, so by the time a dependent runs, its upstreams already resolve to the real
names their SQL references.
"""

from __future__ import annotations

from clair.exceptions import ClairError
from clair.trouves.trouve import TrouveType


STRICT_SUFFIX = "__clair_strict_"

# Snowflake's maximum identifier length.
MAX_IDENTIFIER_LENGTH = 255


class StrictNamingError(ClairError):
    """Raised when the strict staging name would exceed Snowflake's identifier limit."""


def strict_staging_name(full_name: str, run_id: str) -> str:
    """Return the run-scoped staging name for a Trouve's routed full_name.

    The suffix is appended to the table component only, so the staging object
    lives in the same database and schema as its target -- a requirement for
    ``ALTER TABLE ... SWAP WITH ...``.

    Args:
        full_name: Routed "database.schema.table" name of the target object.
        run_id: UUIDv7 hex string identifying this clair run.

    Returns:
        The staging "database.schema.table__clair_strict_<run_id>" name.

    Raises:
        StrictNamingError: If the staging table identifier exceeds 255 characters.
    """
    parts = full_name.split(".")
    if len(parts) != 3:
        raise StrictNamingError(
            f"Cannot derive a strict staging name from '{full_name}': "
            "expected database.schema.table"
        )

    database_name, schema_name, table_name = parts
    staging_table_name = f"{table_name}{STRICT_SUFFIX}{run_id}"

    if len(staging_table_name) > MAX_IDENTIFIER_LENGTH:
        raise StrictNamingError(
            f"Strict staging name '{staging_table_name}' is "
            f"{len(staging_table_name)} chars (max {MAX_IDENTIFIER_LENGTH}). "
            f"Shorten the name of '{full_name}' or run without --strict."
        )

    return f"{database_name}.{schema_name}.{staging_table_name}"


def build_clone_statement(target_name: str, staging_name: str) -> str:
    """Return the zero-copy CLONE that seeds an incremental build's staging table.

    Snowflake clones are metadata-only, so this is constant-time regardless of
    how large the target table is.
    """
    return (
        f"-- strict: clone target into staging so incremental statements have a base\n"
        f"CREATE OR REPLACE TABLE {staging_name} CLONE {target_name}"
    )


def build_promote_statements(
    trouve_type: TrouveType,
    staging_name: str,
    target_name: str,
    target_exists: bool,
    resolved_sql: str = "",
) -> list[str]:
    """Return the statements that promote a tested staging object into its real name.

    Args:
        trouve_type: TABLE or VIEW. SOURCE Trouves are never materialized.
        staging_name: Routed name of the staging object holding tested data.
        target_name: Routed name the object should end up under.
        target_exists: Whether the target already exists in the warehouse.
        resolved_sql: The Trouve's resolved SQL; required for VIEW promotion.

    Returns:
        Ordered list of SQL statements to execute.
    """
    if trouve_type == TrouveType.VIEW:
        # Views have no SWAP. CREATE OR REPLACE VIEW is itself atomic and, being
        # metadata-only, costs nothing -- the staging view proved the SQL is valid
        # and its results pass the tests.
        return [
            f"-- strict: promote tested view\n"
            f"CREATE OR REPLACE VIEW {target_name} AS (\n{resolved_sql.strip()}\n)",
            f"-- strict: drop staging view\n"
            f"DROP VIEW IF EXISTS {staging_name}",
        ]

    if not target_exists:
        return [
            f"-- strict: promote tested table (target did not exist)\n"
            f"ALTER TABLE {staging_name} RENAME TO {target_name}"
        ]

    return [
        # SWAP is a metadata-only operation: O(1) in the size of either table.
        f"-- strict: swap tested staging table into place\n"
        f"ALTER TABLE {staging_name} SWAP WITH {target_name}",
        # After the swap, the staging name holds the previous target contents.
        f"-- strict: drop the superseded table\n"
        f"DROP TABLE IF EXISTS {staging_name}",
    ]


def build_cleanup_statement(trouve_type: TrouveType, staging_name: str) -> str:
    """Return the statement that discards a staging object after a failed build or test."""
    object_type = "VIEW" if trouve_type == TrouveType.VIEW else "TABLE"
    return (
        f"-- strict: discard untested staging object\n"
        f"DROP {object_type} IF EXISTS {staging_name}"
    )
