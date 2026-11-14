"""Staging -- the address where a run writes and tests a Trouve before the promotion.

Snowflake can test a table only after the table exists. A direct write puts
untested data into the physical address, and the tests then report a table that
is already wrong. Thus a staged run writes each Trouve to
``<table>__clair_<run_id>``, tests it there, and promotes it after the tests
pass. A failed test leaves the physical object as it was, and the rejected
candidate stays at the staging address for an examination.

This module gives the staging address. The statements of each step come from
the phases of ``Trouve.plan()``, and ``clair.trouves.statements`` holds their
SQL text.
"""

from __future__ import annotations

from pydantic import ValidationError

from clair.environments.routing import TrouveAddress
from clair.exceptions import ClairError

STAGING_SUFFIX = "__clair_"


class StagingAddressError(ClairError):
    """Clair cannot make a staging address for a Trouve."""


def make_staging_address(physical_address: TrouveAddress, run_id: str) -> TrouveAddress:
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
