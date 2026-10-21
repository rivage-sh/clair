"""The columns of an incremental target, and what clair does when the SQL changes them.

An incremental run writes the new rows into a table that exists. The SQL of the
Trouve can give a column that the table does not hold, or no longer give a
column that it holds. Clair compares the two lists before it writes, by name:

- A new column stops the run with ``OnSchemaChange.FAIL``. With
  ``OnSchemaChange.ADD_NEW_COLUMNS``, clair adds it to the table first.
- A removed column always stops the run. Only a person can decide if its data
  must go.

A staged run adds each column to the staging clone and not to the physical
table. Thus a test that fails leaves the physical table with its old columns
and its old data.
"""

from __future__ import annotations

from pydantic import BaseModel

from clair.adapters.base import WarehouseAdapter, WarehouseColumn
from clair.exceptions import RunError
from clair.trouves.address import TrouveAddress
from clair.trouves.run_config import Incremental, OnSchemaChange
from clair.trouves.trouve import Table, quote_column_name


class SchemaChange(BaseModel):
    """The difference between the columns of the SQL and the columns of the table, by name.

    Attributes:
        added_columns: Each column that the SQL gives and the table does not
            hold, in the order of the SQL.
        removed_column_names: Each column that the table holds and the SQL does
            not give, in the order of the table.
    """

    added_columns: list[WarehouseColumn] = []
    removed_column_names: list[str] = []

    @property
    def is_empty(self) -> bool:
        return not self.added_columns and not self.removed_column_names


class IncrementalColumns(BaseModel):
    """What an incremental build needs to know about its columns.

    Attributes:
        target_column_names: The columns that the INSERT names: each column of
            the table, then each column that clair adds.
        alter_statements: The statements that add each new column, before the build.
        change: The difference that clair found.
    """

    target_column_names: list[str]
    alter_statements: list[str]
    change: SchemaChange


def compare_columns(
    target_columns: list[WarehouseColumn], new_row_columns: list[WarehouseColumn]
) -> SchemaChange:
    """Give the columns that the SQL adds and removes, compared to the table."""
    target_names = {column.name for column in target_columns}
    new_row_names = {column.name for column in new_row_columns}
    return SchemaChange(
        added_columns=[column for column in new_row_columns if column.name not in target_names],
        removed_column_names=[
            column.name for column in target_columns if column.name not in new_row_names
        ],
    )


def schema_change_error(
    change: SchemaChange, on_schema_change: OnSchemaChange, physical_address: TrouveAddress
) -> str | None:
    """Give the error that stops the run, or None if the run can continue."""
    if change.is_empty:
        return None

    lines: list[str] = []
    if change.added_columns and on_schema_change == OnSchemaChange.FAIL:
        added_names = ", ".join(column.name for column in change.added_columns)
        lines.append(f"The SQL gives columns that {physical_address} does not hold: {added_names}.")
    if change.removed_column_names:
        removed_names = ", ".join(change.removed_column_names)
        lines.append(f"{physical_address} holds columns that the SQL does not give: {removed_names}.")
    if not lines:
        return None

    lines.append("Clair writes nothing.")
    if change.removed_column_names:
        lines.append(
            "A removed column needs a person: drop the column from the table, or make "
            "the table again with clair run --run-mode full_refresh."
        )
    else:
        lines.append(
            "To add the new columns in an incremental run, give "
            "Incremental(on_schema_change=OnSchemaChange.ADD_NEW_COLUMNS). To make the "
            "table again, run clair run --run-mode full_refresh."
        )
    return "\n".join(lines)


def add_column_statements(
    change: SchemaChange, address: TrouveAddress, object_keyword: str
) -> list[str]:
    """Give one ALTER for each column that the SQL adds.

    *object_keyword* is ``TABLE`` or ``ICEBERG TABLE``.
    """
    return [
        f"-- add the new column {column.name}\n"
        f"ALTER {object_keyword} {address} "
        f"ADD COLUMN {quote_column_name(column.name)} {column.data_type}"
        for column in change.added_columns
    ]


def read_incremental_columns(
    trouve: Table,
    adapter: WarehouseAdapter,
    physical_address: TrouveAddress,
    write_address: TrouveAddress,
) -> IncrementalColumns:
    """Read the columns of the table and of the SQL, and apply the policy of the Trouve.

    Args:
        trouve: An incremental Table.
        adapter: The connection that reads the two lists of columns.
        physical_address: The table that holds the rows of the earlier runs.
        write_address: The table that the build writes to: the staging clone,
            or the physical table in a run with no staging.

    Raises:
        RunError: If clair cannot read a list of columns, or the policy stops
            the run.
    """
    if not isinstance(trouve.run_config, Incremental):
        raise RunError(f"{physical_address} has no incremental run_config.")

    target_columns = adapter.describe_columns(physical_address)
    if target_columns is None:
        raise RunError(f"Clair cannot read the columns of {physical_address}.")

    new_row_columns = adapter.describe_query(trouve.resolved_sql())
    change = compare_columns(target_columns, new_row_columns)
    error = schema_change_error(change, trouve.run_config.on_schema_change, physical_address)
    if error is not None:
        raise RunError(error)

    return IncrementalColumns(
        target_column_names=[
            *(column.name for column in target_columns),
            *(column.name for column in change.added_columns),
        ],
        alter_statements=add_column_statements(change, write_address, trouve.object_keyword),
        change=change,
    )

