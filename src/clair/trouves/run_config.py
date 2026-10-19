"""The run configuration of a Trouve: a full refresh, or an incremental run.

``RunConfig`` is ``FullRefresh | Incremental``. A full refresh takes no
parameter. An incremental run takes a strategy, and each strategy holds only the
parameters that it reads. Thus an illegal combination, for example a key column
on an APPEND, has no way to exist.

A subclass of ``Table`` that overrides ``build_sql()`` writes its own
incremental SQL. It gives ``Incremental()`` with no strategy.
"""

from __future__ import annotations

from enum import StrEnum
from typing import Annotated, ClassVar, Literal

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


def _or_list(names: tuple[str, ...]) -> str:
    """Give "a or b", or "a, b or c"."""
    return " or ".join([", ".join(names[:-1]), names[-1]])


class _MatchingStrategy(_ConfigModel):
    """A strategy that matches a new row to a row of the target table.

    Attributes:
        primary_key_columns: The columns to match on. Clair makes
            ``target.column = source.column`` for each one.
        join_sql: Your own match condition, with the aliases ``target`` and
            ``source``. Use it in place of primary_key_columns.
    """

    # The fields that each select the target rows. A strategy takes one of them.
    match_field_names: ClassVar[tuple[str, ...]] = ("primary_key_columns", "join_sql")

    primary_key_columns: list[str] | None = None
    join_sql: str | None = None

    @model_validator(mode="after")
    def _give_exactly_one_match(self) -> _MatchingStrategy:
        strategy_name = type(self).__name__
        given_names = [
            field_name
            for field_name in self.match_field_names
            if getattr(self, field_name) is not None
        ]
        if not given_names:
            raise ValueError(f"{strategy_name} needs {_or_list(self.match_field_names)}")
        if len(given_names) > 1:
            raise ValueError(
                f"{strategy_name} takes only one of {_or_list(self.match_field_names)}, "
                f"but it got {' and '.join(given_names)}"
            )
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
    """Delete the target rows that the new rows replace, then insert the new rows.

    A key or a join deletes each target row that a new row matches. A range
    column deletes each target row in the range of the new rows, thus it also
    removes a row that the new rows do not hold any more.

    Attributes:
        range_column: Delete each target row whose value of this column is
            between the minimum and the maximum of the new rows. Use it in
            place of primary_key_columns or join_sql.
    """

    match_field_names: ClassVar[tuple[str, ...]] = (
        "primary_key_columns",
        "join_sql",
        "range_column",
    )

    mode: Literal[IncrementalMode.DELETE_INSERT] = IncrementalMode.DELETE_INSERT
    range_column: str | None = None


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
