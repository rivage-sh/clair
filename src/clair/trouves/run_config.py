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


# The aliases of the target table and the source table in a MERGE statement.
TARGET = "target"
SOURCE = "source"


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
        primary_key_columns: The column names that UPSERT matches on. Clair makes
            the ON clause from them.
        join_sql: Your own ON clause for UPSERT. Use it in place of
            primary_key_columns.
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
            raise ValueError("incremental_mode is only valid when run_mode is incremental")
        if self.run_mode == RunMode.INCREMENTAL and self.incremental_mode is None:
            raise ValueError("incremental run_mode requires incremental_mode")
        if self.incremental_mode == IncrementalMode.APPEND:
            if self.primary_key_columns is not None:
                raise ValueError("primary_key_columns is only valid for upsert mode")
            if self.join_sql is not None:
                raise ValueError("join_sql is only valid for upsert mode")
            if self.upsert_config is not None:
                raise ValueError("upsert_config is only valid for upsert mode")
        if self.incremental_mode == IncrementalMode.UPSERT:
            if self.primary_key_columns is not None and self.join_sql is not None:
                raise ValueError("specify primary_key_columns or join_sql, not both")
            if self.primary_key_columns is None and self.join_sql is None:
                raise ValueError("upsert mode requires primary_key_columns or join_sql")
        return self
