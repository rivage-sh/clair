"""The run configuration types for incremental materializations."""

from __future__ import annotations

from enum import StrEnum

from pydantic import BaseModel, model_validator


class RunMode(StrEnum):
    """The method that `clair run` uses to materialize a Trouve."""
    FULL_REFRESH = "full_refresh"
    INCREMENTAL = "incremental"


class IncrementalMode(StrEnum):
    """The method that clair uses to apply incremental data."""
    APPEND = "append"
    UPSERT = "upsert"
    DELETE_INSERT = "delete_insert"


# The modes that match a new row to a row of the target table. Each one needs
# primary_key_columns or join_sql.
MATCHING_MODES = frozenset({IncrementalMode.UPSERT, IncrementalMode.DELETE_INSERT})


# The aliases of the target table and the source table in a MERGE or a DELETE.
TARGET = "target"
SOURCE = "source"


def match_condition(join_sql: str | None, key_columns: list[str]) -> str:
    """Give the condition that matches a target row to a source row.

    Args:
        join_sql: The condition that the author wrote, or None.
        key_columns: The key columns that make the condition, when the author
            wrote no join_sql.

    Returns:
        The join_sql of the author, or an equality on each key column.
    """
    if join_sql:
        return join_sql
    return " AND ".join(
        f"{TARGET}.{column_name} = {SOURCE}.{column_name}" for column_name in key_columns
    )


class UpsertConfig(BaseModel):
    """Exact column control for an UPSERT MERGE statement.

    Use this class when you set ``join_sql`` and clair cannot find the join keys.
    Use it also when clair must update or insert only some of the columns.

    Attributes:
        update_columns: The columns for the WHEN MATCHED THEN UPDATE SET clause.
            The default is all the columns that are not primary keys. If you set
            join_sql, the default is all the columns.
        insert_columns: The columns for the WHEN NOT MATCHED THEN INSERT clause.
            The default is all the columns.
    """

    update_columns: list[str] | None = None
    insert_columns: list[str] | None = None


class RunConfig(BaseModel):
    """This class controls how clair materializes a Trouve.

    Attributes:
        run_mode: FULL_REFRESH makes the table again. INCREMENTAL applies only
            the new data.
        incremental_mode: APPEND inserts new rows. UPSERT merges on a key.
            DELETE_INSERT deletes each target row that a new row matches, then
            inserts the new rows.
        primary_key_columns: The column names that UPSERT and DELETE_INSERT
            match on. Clair makes the match condition from them.
        join_sql: Your own match condition for UPSERT and DELETE_INSERT. Use
            it in place of primary_key_columns.
        upsert_config: Optional column overrides for the UPSERT MERGE statement.
    """

    run_mode: RunMode = RunMode.FULL_REFRESH
    incremental_mode: IncrementalMode | None = None
    primary_key_columns: list[str] | None = None
    join_sql: str | None = None
    upsert_config: UpsertConfig | None = None

    @model_validator(mode="after")
    def _validate_config(self) -> RunConfig:
        if self.run_mode == RunMode.FULL_REFRESH and self.incremental_mode is not None:
            raise ValueError("you can set incremental_mode only when run_mode is incremental")
        if self.run_mode == RunMode.INCREMENTAL and self.incremental_mode is None:
            raise ValueError("the incremental run_mode needs incremental_mode")
        if self.incremental_mode == IncrementalMode.APPEND:
            if self.primary_key_columns is not None:
                raise ValueError(
                    "you can set primary_key_columns only for the upsert mode "
                    "and the delete_insert mode"
                )
            if self.join_sql is not None:
                raise ValueError(
                    "you can set join_sql only for the upsert mode and the delete_insert mode"
                )
        if self.upsert_config is not None and self.incremental_mode in (
            IncrementalMode.APPEND,
            IncrementalMode.DELETE_INSERT,
        ):
            raise ValueError("you can set upsert_config only for the upsert mode")
        if self.incremental_mode in MATCHING_MODES:
            if self.primary_key_columns is not None and self.join_sql is not None:
                raise ValueError("set primary_key_columns or join_sql, but not both")
            if self.primary_key_columns is None and self.join_sql is None:
                raise ValueError(
                    f"the {self.incremental_mode} mode needs primary_key_columns or join_sql"
                )
        return self
