"""Strict mode -- materialize into a run-scoped staging object, test, then promote.

A table can only be tested once it has been materialized, so a plain run leaves a
window where the production object holds untested data. Strict mode closes that
window:

1. Materialize the Trouve into ``<table>__clair_<run_id>``, a sibling object in the
   same schema. For incremental Trouves the current target is first zero-copy
   cloned into that name so the incremental statements have a base to apply to.
2. Run the Trouve's data quality tests against the staging object.
3. On pass, promote the staging object into the real name with
   ``CREATE OR REPLACE TABLE <target> CLONE <staging> COPY GRANTS`` -- a
   metadata-only operation whose cost does not scale with table size.
4. On failure, leave the staging object in place. The production object is never
   touched, and the rejected candidate can be queried directly to find out why.

Downstream Trouves are unaffected: promotion happens immediately after each node's
tests, so by the time a dependent runs, its upstreams already resolve to the real
names their SQL references.
"""

from __future__ import annotations

from clair.exceptions import ClairError
from clair.trouves.trouve import TrouveType


STRICT_SUFFIX = "__clair_"

# Snowflake's maximum identifier length.
#
# Verified against a live account: the limit applies to each object name
# individually, not to the fully-qualified path. A 255-character table name is
# accepted; 256 is rejected with "Object name '...' exceeds maximum length limit
# of 255 characters"; and a 767-character database.schema.table (255 per
# component) creates and queries without complaint. Only the table component
# grows under strict mode, so that is the only one checked below.
MAX_IDENTIFIER_LENGTH = 255


class StrictNamingError(ClairError):
    """Raised when the strict staging name would exceed Snowflake's identifier limit."""


def strict_staging_name(full_name: str, run_id: str) -> str:
    """Return the run-scoped staging name for a Trouve's routed full_name.

    The suffix is appended to the table component only, so the staging object
    lives in the same database and schema as its target. Promotion clones rather
    than swaps, so this is a convention rather than a hard requirement -- but it
    keeps a rejected candidate next to the table it was meant to become.

    Args:
        full_name: Routed "database.schema.table" name of the target object.
        run_id: UUIDv7 hex string identifying this clair run.

    Returns:
        The staging "database.schema.table__clair_<run_id>" name.

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


def build_promote_statement(
    trouve_type: TrouveType,
    staging_name: str,
    target_name: str,
    resolved_sql: str = "",
) -> str:
    """Return the statement that promotes a tested staging object into its real name.

    ``COPY GRANTS`` is what makes this safe to run against a production object.
    Without it, privileges granted directly on the target are lost: they are
    attached to the object, not the name, so ``ALTER TABLE ... SWAP WITH`` carries
    them off under the staging name and leaves the production name bare. With
    ``COPY GRANTS``, Snowflake copies every privilege except OWNERSHIP from the
    object being replaced -- or, when the target does not exist yet, from the
    clone source. That covers both cases without a branch.

    OWNERSHIP is the one privilege that does not carry over; it lands on the role
    executing the run. ``SWAP`` behaves the same way, so this is not a regression,
    but a target owned by some other role will change hands.

    Args:
        trouve_type: TABLE or VIEW. SOURCE Trouves are never materialized.
        staging_name: Routed name of the staging object holding tested data.
        target_name: Routed name the object should end up under.
        resolved_sql: The Trouve's resolved SQL; required for VIEW promotion.

    Returns:
        A single SQL statement.
    """
    if trouve_type == TrouveType.VIEW:
        # Views cannot be cloned into place the way tables can, but CREATE OR
        # REPLACE VIEW is itself atomic and metadata-only -- the staging view
        # proved the SQL is valid and that its results pass the tests.
        return (
            f"-- strict: promote tested view\n"
            f"CREATE OR REPLACE VIEW {target_name} COPY GRANTS AS (\n"
            f"{resolved_sql.strip()}\n)"
        )

    # A clone is metadata-only: O(1) in the size of the staging table.
    return (
        f"-- strict: promote tested table\n"
        f"CREATE OR REPLACE TABLE {target_name} CLONE {staging_name} COPY GRANTS"
    )


def build_drop_staging_statement(trouve_type: TrouveType, staging_name: str) -> str:
    """Return the statement that drops a staging object after a successful promotion.

    Only ever used on the success path. A staging object left behind by a failed
    build or a failed test is deliberately retained -- it is the only copy of the
    rejected candidate, and reproducing it means re-running everything upstream.
    """
    object_type = "VIEW" if trouve_type == TrouveType.VIEW else "TABLE"
    return (
        f"-- strict: drop the promoted staging object\n"
        f"DROP {object_type} IF EXISTS {staging_name}"
    )
