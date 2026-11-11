"""Prune -- drop the objects that one run left in the warehouse.

A run writes two kinds of object for itself, and the name of each ends with
the run id:

- the staging object of a Trouve, ``<table>__clair_<run_id>``. Clair drops it
  after the promotion, and keeps it after a failure, for your examination.
- the new rows table of an ``Upsert`` or a ``DeleteInsert``,
  ``<table>__clair_new_rows_<run_id>``. Clair drops it at the end of the node,
  but a run that stops in the middle keeps it.

A full refresh candidate is a complete copy of its table, thus these objects
cost storage until somebody drops them. The run id is unique, thus a search by
name finds each of them, also for a Trouve that the project no longer holds.
"""

from __future__ import annotations

import re

from pydantic import BaseModel

from clair.adapters.base import NamedWarehouseObject, WarehouseAdapter
from clair.adapters.objects import Statement, WarehouseObjectType
from clair.core.staging import STAGING_SUFFIX
from clair.exceptions import ClairError
from clair.trouves.trouve import NEW_ROWS_SUFFIX

# uuid7().hex gives 32 hexadecimal characters. Snowflake shows an unquoted
# name in upper case, thus a run id that you copy from it can be upper case.
_RUN_ID_PATTERN = re.compile(r"[0-9a-fA-F]{32}")


class InvalidRunIdError(ClairError):
    """A text that is not the id of a clair run."""


class PrunedObject(BaseModel):
    """One object of the run, and the statement that dropped it.

    Attributes:
        warehouse_object: The address and the type of the object.
        statement: The DROP statement and its result. None in a dry run.
    """

    warehouse_object: NamedWarehouseObject
    statement: Statement | None = None


class PruneSummary(BaseModel):
    """The result of one prune operation."""

    run_id: str
    dry_run: bool
    objects: list[PrunedObject]

    @property
    def failed(self) -> list[PrunedObject]:
        """Each object whose DROP statement failed."""
        return [
            pruned
            for pruned in self.objects
            if pruned.statement is not None and not pruned.statement.success
        ]

    def render(self) -> str:
        """Make the text for stdout."""
        if not self.objects:
            return f"Clair found no object of the run {self.run_id}."
        verb = "Clair will drop" if self.dry_run else "Clair dropped"
        lines = [f"{verb} {len(self.objects)} object(s) of the run {self.run_id}:"]
        for pruned in self.objects:
            warehouse_object = pruned.warehouse_object
            line = f"  {warehouse_object.object_type.value.upper()} {warehouse_object.address}"
            if pruned.statement is not None and not pruned.statement.success:
                line += f" ... FAILED: {pruned.statement.error}"
            lines.append(line)
        return "\n".join(lines)


def require_a_run_id(run_id: str) -> None:
    """Stop a text that is not a run id.

    An empty or a short text would match the names of many objects, and a
    prune drops each match.

    Raises:
        InvalidRunIdError: If *run_id* is not 32 hexadecimal characters.
    """
    if not _RUN_ID_PATTERN.fullmatch(run_id):
        raise InvalidRunIdError(
            f"'{run_id}' is not a run id. A run id is 32 hexadecimal characters, for "
            "example the end of a staging name <table>__clair_<run_id>."
        )


def _is_object_of_the_run(warehouse_object: NamedWarehouseObject, run_id: str) -> bool:
    """Tell if the name of the object ends with a suffix that a run of *run_id* writes."""
    name = warehouse_object.address.table_name.upper()
    suffixes = (f"{STAGING_SUFFIX}{run_id}".upper(), f"{NEW_ROWS_SUFFIX}{run_id}".upper())
    return name.endswith(suffixes)


def _drop_statement(warehouse_object: NamedWarehouseObject) -> str:
    """Give the DROP statement for the type of the object."""
    object_keyword = None
    if warehouse_object.object_type == WarehouseObjectType.VIEW:
        object_keyword = "VIEW"
    elif warehouse_object.object_type in (
        WarehouseObjectType.TABLE,
        WarehouseObjectType.ICEBERG_TABLE,
    ):
        object_keyword = "TABLE"
    else:
        raise ClairError(f"Clair cannot drop an object of the type {warehouse_object.object_type}.")
    return f"DROP {object_keyword} IF EXISTS {warehouse_object.address}"


def prune_run(adapter: WarehouseAdapter, run_id: str, *, dry_run: bool = False) -> PruneSummary:
    """Find each object of one run, and drop it.

    Args:
        adapter: A warehouse adapter with an open connection.
        run_id: The id of the run.
        dry_run: If True, find the objects, and drop nothing.

    Raises:
        InvalidRunIdError: If *run_id* is not a run id.
        RunError: If the warehouse cannot search for the objects.
    """
    require_a_run_id(run_id)
    run_id = run_id.lower()
    # Each suffix of the run ends with the run id, thus one search finds both.
    found = [
        warehouse_object
        for warehouse_object in adapter.find_objects(run_id)
        if _is_object_of_the_run(warehouse_object, run_id)
    ]
    found.sort(key=lambda warehouse_object: str(warehouse_object.address))

    pruned_objects = [
        PrunedObject(
            warehouse_object=warehouse_object,
            statement=None if dry_run else adapter.execute(_drop_statement(warehouse_object)),
        )
        for warehouse_object in found
    ]
    return PruneSummary(run_id=run_id, dry_run=dry_run, objects=pruned_objects)
