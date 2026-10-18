"""The run configuration of a Trouve: a full refresh, or an incremental run.

``RunConfig`` is ``FullRefresh | Incremental``. A full refresh takes no
parameter. An incremental run takes a strategy, and each strategy holds only the
parameters that it reads. Thus an illegal combination, for example a key column
on an APPEND, has no way to exist.

A subclass of ``Trouve`` that overrides ``build_sql()`` writes its own
incremental SQL. It gives ``Incremental()`` with no strategy.
"""

from __future__ import annotations

from enum import StrEnum
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class RunMode(StrEnum):
    """The method that `clair run` uses to materialize a Trouve."""
    FULL_REFRESH = "full_refresh"
    INCREMENTAL = "incremental"


class IncrementalMode(StrEnum):
    """The name of each built-in incremental strategy."""
    APPEND = "append"
    UPSERT = "upsert"
    DELETE_INSERT = "delete_insert"


# The aliases of the target table and the source table in a MERGE or a DELETE.
TARGET = "target"
SOURCE = "source"


class _ConfigModel(BaseModel):
    # A field name of the old flat RunConfig, for example incremental_mode,
    # must fail, and not disappear in silence.
    model_config = ConfigDict(extra="forbid", frozen=True)


class Append(_ConfigModel):
    """Insert the new rows, and keep each row that the table holds."""

    mode: Literal[IncrementalMode.APPEND] = IncrementalMode.APPEND


class _MatchingStrategy(_ConfigModel):
    """A strategy that matches a new row to a row of the target table.

    Attributes:
        primary_key_columns: The columns to match on. Clair makes
            ``target.column = source.column`` for each one.
        join_sql: Your own match condition, with the aliases ``target`` and
            ``source``. Use it in place of primary_key_columns.
    """

    primary_key_columns: list[str] | None = None
    join_sql: str | None = None

    @model_validator(mode="after")
    def _give_exactly_one_match(self) -> _MatchingStrategy:
        strategy_name = type(self).__name__
        if self.primary_key_columns is not None and self.join_sql is not None:
            raise ValueError(
                f"{strategy_name} takes primary_key_columns or join_sql, but not both"
            )
        if self.primary_key_columns is None and self.join_sql is None:
            raise ValueError(f"{strategy_name} needs primary_key_columns or join_sql")
        return self

    @property
    def match_condition(self) -> str:
        """Give the join_sql of the author, or an equality on each key column."""
        if self.join_sql:
            return self.join_sql
        return " AND ".join(
            f"{TARGET}.{column_name} = {SOURCE}.{column_name}"
            for column_name in self.primary_key_columns or []
        )


class Upsert(_MatchingStrategy):
    """Merge the new rows: update each row that matches, and insert each other row.

    Attributes:
        update_columns: The columns of the WHEN MATCHED THEN UPDATE SET clause.
            The default is each column that is not a primary key column. With
            join_sql, the default is each column.
        insert_columns: The columns of the WHEN NOT MATCHED THEN INSERT clause.
            The default is each column.
    """

    mode: Literal[IncrementalMode.UPSERT] = IncrementalMode.UPSERT
    update_columns: list[str] | None = None
    insert_columns: list[str] | None = None


class DeleteInsert(_MatchingStrategy):
    """Delete each target row that a new row matches, then insert the new rows."""

    mode: Literal[IncrementalMode.DELETE_INSERT] = IncrementalMode.DELETE_INSERT


IncrementalStrategy = Annotated[Append | Upsert | DeleteInsert, Field(discriminator="mode")]


class FullRefresh(_ConfigModel):
    """Make the object again on each run. This is the default."""

    run_mode: Literal[RunMode.FULL_REFRESH] = RunMode.FULL_REFRESH


class Incremental(_ConfigModel):
    """Apply the new data only, to the object that exists.

    Attributes:
        strategy: The built-in method that writes the new data. None tells
            clair that a ``build_sql()`` override writes the SQL.
    """

    run_mode: Literal[RunMode.INCREMENTAL] = RunMode.INCREMENTAL
    strategy: IncrementalStrategy | None = None


RunConfig = Annotated[FullRefresh | Incremental, Field(discriminator="run_mode")]
