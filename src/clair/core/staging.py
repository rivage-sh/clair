"""Staging -- write each Trouve to a temporary address, test it, then promote it.

Snowflake can test a table only after the table exists. A direct write puts
untested data into the physical address, and the tests then report a table that
is already wrong. Clair writes through a staging address instead:

1. Clair materializes the Trouve at ``<table>__clair_<run_id>``. This address
   sits in the database and the schema of the physical address. An incremental
   Trouve first gets a zero-copy clone of the physical table, because the
   incremental statements need a base.
2. The data quality tests of the Trouve run against the staging object.
3. The tests pass. Clair promotes the staging object with
   ``CREATE OR REPLACE TABLE <physical> CLONE <staging> COPY GRANTS``. Snowflake
   does this in the metadata, so the time does not grow with the table size.
4. The tests fail. The staging object stays. The physical object keeps the data
   that it had, and you can query the rejected candidate to find the cause.

A downstream Trouve reads the physical address, because clair promotes each node
immediately after the tests of that node. The SQL of a dependent therefore finds
the data that it expects.
"""

from __future__ import annotations

from pydantic import ValidationError

from clair.environments.routing import TrouveAddress
from clair.exceptions import ClairError
from clair.trouves.trouve import TrouveType

STAGING_SUFFIX = "__clair_"


class StagingAddressError(ClairError):
    """Clair cannot make a staging address for a Trouve."""


def staging_address(physical_address: TrouveAddress, run_id: str) -> TrouveAddress:
    """Give the run-scoped staging address for a physical address.

    The suffix goes on the table name only, so the staging object stays in the
    database and the schema of the physical object. Promotion clones, and a
    clone accepts a different schema, so this is a convention. It keeps a
    rejected candidate beside the table that it must become.

    ``TrouveAddress`` applies the Snowflake identifier rules, so a table name
    that the suffix makes too long fails here. The run stops before any SQL
    runs. Note that ``model_copy`` does not validate, so this code makes a new
    address.

    Args:
        physical_address: The address that clair writes to after the tests pass.
        run_id: The UUIDv7 hex identifier of this clair run.

    Returns:
        The staging address.

    Raises:
        StagingAddressError: If the staging table name breaks an identifier rule.
    """
    staging_table_name = f"{physical_address.table_name}{STAGING_SUFFIX}{run_id}"

    try:
        return TrouveAddress(
            database_name=physical_address.database_name,
            schema_name=physical_address.schema_name,
            table_name=staging_table_name,
        )
    except ValidationError as validation_error:
        raise StagingAddressError(
            f"Clair cannot make a staging address for '{physical_address}'. "
            f"The staging table name '{staging_table_name}' is not valid: "
            f"{validation_error.errors()[0]['msg']} "
            f"Give the Trouve a shorter name."
        ) from validation_error


def build_clone_statement(
    physical_address: TrouveAddress, staging_address: TrouveAddress
) -> str:
    """Give the zero-copy clone that seeds the staging table of an incremental run.

    Snowflake makes a clone in the metadata, so the time does not grow with the
    size of the physical table.
    """
    return (
        f"-- staging: clone the target, so the incremental statements have a base\n"
        f"CREATE OR REPLACE TABLE {staging_address} CLONE {physical_address}"
    )


def build_promote_statement(
    trouve_type: TrouveType,
    staging_address: TrouveAddress,
    physical_address: TrouveAddress,
    resolved_sql: str = "",
) -> str:
    """Give the statement that promotes a tested staging object to its physical address.

    ``COPY GRANTS`` makes this operation safe against a production object.
    Without it clair removes each privilege that an administrator granted on the
    target, because Snowflake attaches a privilege to the object and not to the
    name. An ``ALTER TABLE ... SWAP WITH`` moves the grants to the staging name
    and leaves the physical name bare. With ``COPY GRANTS`` Snowflake copies each
    privilege except OWNERSHIP from the object that it replaces. If the physical
    object does not exist, Snowflake copies from the clone source. One statement
    thus covers the two conditions.

    OWNERSHIP is the exception. It goes to the role that runs clair. ``SWAP`` has
    the same result, so this is not a new fault, but a target that a different
    role owns changes owner.

    Args:
        trouve_type: TABLE or VIEW. Clair never materializes a SOURCE Trouve.
        staging_address: The address of the object that holds the tested data.
        physical_address: The address that the object must have.
        resolved_sql: The resolved SQL of the Trouve. A VIEW needs it.

    Returns:
        One SQL statement.
    """
    if trouve_type == TrouveType.VIEW:
        # Snowflake cannot clone a view into position. But CREATE OR REPLACE VIEW
        # is atomic and it operates in the metadata. The staging view showed that
        # the SQL is correct and that the results pass the tests.
        return (
            f"-- staging: promote the tested view\n"
            f"CREATE OR REPLACE VIEW {physical_address} COPY GRANTS AS (\n"
            f"{resolved_sql.strip()}\n)"
        )

    # Snowflake makes a clone in the metadata. The time is constant.
    return (
        f"-- staging: promote the tested table\n"
        f"CREATE OR REPLACE TABLE {physical_address} CLONE {staging_address} COPY GRANTS"
    )


def build_drop_staging_statement(
    trouve_type: TrouveType, staging_address: TrouveAddress
) -> str:
    """Give the statement that drops a staging object after clair promotes it.

    Clair uses this statement only after a promotion. A staging object that a
    failed build or a failed test leaves is kept: it is the only copy of the
    rejected candidate, and a new copy needs a new run of each upstream Trouve.
    """
    object_type = "VIEW" if trouve_type == TrouveType.VIEW else "TABLE"
    return (
        f"-- staging: drop the staging object that clair promoted\n"
        f"DROP {object_type} IF EXISTS {staging_address}"
    )
