"""Run configuration types for incremental materializations."""

from __future__ import annotations

from enum import StrEnum

from pydantic import BaseModel, model_validator


class RunMode(StrEnum):
    """How a Trouve is materialized during `clair run`."""
    FULL_REFRESH = "full_refresh"
    INCREMENTAL = "incremental"


class IncrementalMode(StrEnum):
    """Strategy for applying incremental data."""
    APPEND = "append"
    UPSERT = "upsert"


# Importable aliases for MERGE statement table aliases.
TARGET = "target"
SOURCE = "source"


class UpsertConfig(BaseModel):
    """Fine-grained column control for UPSERT MERGE statements.

    Useful when ``join_sql`` is used and clair cannot infer which columns are
    join keys, or when only a subset of columns should be updated or inserted.

    Attributes:
        update_columns: Columns to include in WHEN MATCHED THEN UPDATE SET.
            Defaults to all non-primary-key columns (or all columns when join_sql is used).
        insert_columns: Columns to include in WHEN NOT MATCHED THEN INSERT.
            Defaults to all columns.
    """

    update_columns: list[str] | None = None
    insert_columns: list[str] | None = None


class RunConfig(BaseModel):
    """Controls how a Trouve is materialized.

    Attributes:
        run_mode: FULL_REFRESH recreates the table; INCREMENTAL applies only new data.
        incremental_mode: APPEND inserts new rows; UPSERT merges on key.
        primary_key_columns: Column names to match on for UPSERT (generates ON clause).
        join_sql: Custom ON clause for UPSERT (alternative to primary_key_columns).
        upsert_config: Optional column overrides for UPSERT MERGE statements.
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
