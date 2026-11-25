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

import uuid6
from pydantic import ValidationError

from clair.environments.routing import TrouveAddress
from clair.exceptions import ClairError

STAGING_SUFFIX = "__clair_"


def new_run_id() -> str:
    """Give the identifier of a new run: the hex form of a UUIDv7.

    A UUIDv7 starts with a millisecond timestamp, thus the identifiers sort by
    time. Each run object name holds it, thus its length decides if a name fits.
    """
    return uuid6.uuid7().hex


# The run id of a plan that sends nothing, such as the plans of clair validate.
# It has the length of a real run id, thus each run object name has the length
# that a run gives it.
PLACEHOLDER_RUN_ID = "0" * len(new_run_id())


class StagingAddressError(ClairError):
    """Clair cannot make the address of an object that one run makes for itself."""


def make_staging_address(physical_address: TrouveAddress, run_id: str) -> TrouveAddress:
    """Give the run-scoped staging address for a physical address.

    The suffix goes on the table name only, so the staging object stays in the
    database and the schema of the physical object. Promotion clones, and a
    clone accepts a different schema, so this is a convention. It keeps a
    rejected candidate beside the table that it must become.

    Args:
        physical_address: The address that clair writes to after the tests pass.
        run_id: The UUIDv7 hex identifier of this clair run.

    Returns:
        The staging address.

    Raises:
        StagingAddressError: If the staging table name breaks an identifier rule.
    """
    return make_run_scoped_address(physical_address, STAGING_SUFFIX, run_id)


def make_run_scoped_address(
    physical_address: TrouveAddress, suffix: str, run_id: str
) -> TrouveAddress:
    """Give the address of an object that one run makes beside the physical object.

    ``TrouveAddress`` applies the Snowflake identifier rules, so a table name
    that the suffix makes too long fails here, before any SQL runs. Note that
    ``model_copy`` does not validate, so this code makes a new address.

    Args:
        physical_address: The address of the Trouve.
        suffix: The text between the table name and the run id, for example
            ``STAGING_SUFFIX``.
        run_id: The UUIDv7 hex identifier of this clair run.

    Raises:
        StagingAddressError: If the table name breaks an identifier rule.
    """
    scoped_table_name = f"{physical_address.table_name}{suffix}{run_id}"

    try:
        return TrouveAddress(
            database_name=physical_address.database_name,
            schema_name=physical_address.schema_name,
            table_name=scoped_table_name,
        )
    except ValidationError as validation_error:
        raise StagingAddressError(
            f"Clair cannot make the address of a run object for '{physical_address}'. "
            f"The table name '{scoped_table_name}' is not valid: "
            f"{validation_error.errors()[0]['msg']} "
            f"Give the Trouve a shorter name."
        ) from validation_error
