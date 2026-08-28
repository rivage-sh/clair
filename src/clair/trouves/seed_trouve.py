"""SeedTrouve -- a Trouve that holds its rows in the Python file."""

from __future__ import annotations

import pandas as pd
from pydantic import Field, model_validator

from clair.trouves.dataframe_trouve import DataframeTrouve
from clair.trouves.run_config import RunMode
from clair.trouves.trouve import TrouveAbc, TrouveType

# The artifact file of `clair compile` shows this number of rows maximum. A
# seed with more rows keeps the first rows and the last rows.
_ARTIFACT_MAX_ROWS = 50


class SeedTrouve(DataframeTrouve):
    """A Trouve that holds its rows in the Python file.

    Use it for a small table that a person maintains by hand: country codes,
    tax rates, or a map from an id to a label. Clair writes the DataFrame to
    the warehouse. The DAG, the lineage, the selectors, and the data quality
    tests all apply, the same as for a SQL Trouve.

    A seed reads no other Trouve, thus it is always a root of the DAG. Clair
    builds it in the same run as every other Trouve. There is no separate
    command.

    The Snowflake type of each column comes from the dtype of that column. To
    control a type, set the dtype in Python. The ``columns`` attribute stays
    documentation, the same as for every other Trouve.

    Example:
        >>> frame = pd.DataFrame({"code": ["US", "FR"], "rate": [0.0, 0.20]})
        >>> trouve = SeedTrouve(dataframe=frame)

    Attributes:
        dataframe: The rows that clair writes. Clair reads this attribute when
            it imports the file, thus keep the number of rows small.

    ``TrouveAbc`` holds the attributes that every backend shares.
    """

    dataframe: pd.DataFrame = Field(exclude=True)

    def upstream_trouves(self) -> list[TrouveAbc]:
        """A seed reads no other Trouve. This list is always empty."""
        return []

    def build_dataframe(self, *input_dataframes: pd.DataFrame) -> pd.DataFrame:
        """Give the rows. A seed has no input, thus it ignores the arguments."""
        return self.dataframe

    def source_text(self) -> str:
        """Give the dtypes and the rows. This text is the source of a seed."""
        dtype_lines = "\n".join(
            f"#   {column_name}: {dtype}"
            for column_name, dtype in self.dataframe.dtypes.items()
        )
        row_count = len(self.dataframe)
        return (
            f"# {row_count} row(s), {len(self.dataframe.columns)} column(s)\n"
            f"# dtypes:\n{dtype_lines}\n\n"
            f"{self.dataframe.to_string(max_rows=_ARTIFACT_MAX_ROWS)}\n"
        )

    @model_validator(mode="after")
    def _validate_dataframe(self) -> SeedTrouve:
        if self.type != TrouveType.TABLE:
            raise ValueError(
                f"SeedTrouve must be TABLE type, got '{self.type.value}'"
            )
        if self.run_config.run_mode == RunMode.INCREMENTAL:
            raise ValueError("SeedTrouve does not support incremental mode")

        column_names = list(self.dataframe.columns)
        if not column_names:
            raise ValueError("SeedTrouve needs a DataFrame with one column minimum")

        not_text = [name for name in column_names if not isinstance(name, str)]
        if not_text:
            names = ", ".join(repr(name) for name in not_text)
            raise ValueError(
                f"each column name of the DataFrame must be a string, found: {names}"
            )

        seen: set[str] = set()
        duplicates: list[str] = []
        for column_name in column_names:
            if column_name in seen and column_name not in duplicates:
                duplicates.append(column_name)
            seen.add(column_name)
        if duplicates:
            raise ValueError(
                f"the DataFrame has a duplicate column name: {', '.join(duplicates)}"
            )
        return self
