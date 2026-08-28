"""DataframeTrouve -- the base of each Trouve that clair writes from a DataFrame."""

from __future__ import annotations

from abc import ABC, abstractmethod

import pandas as pd

from clair.trouves.trouve import ExecutionType, TrouveAbc


class DataframeTrouve(TrouveAbc, ABC):
    """The base of each Trouve that clair materializes from a DataFrame.

    Clair reads each upstream Trouve into a DataFrame, calls
    ``build_dataframe``, and writes the result to the warehouse. Clair
    executes no SQL for these Trouves, thus the Snowflake type of each column
    comes from the dtype of that column.

    ``PandasTrouve`` calls a function that you write. ``SeedTrouve`` holds its
    rows in the Python file.
    """

    @property
    def execution_type(self) -> ExecutionType:
        return ExecutionType.PANDAS

    @abstractmethod
    def build_dataframe(self, *input_dataframes: pd.DataFrame) -> pd.DataFrame:
        """Give the DataFrame that clair writes to the warehouse.

        Args:
            input_dataframes: One DataFrame for each Trouve that
                ``upstream_trouves`` gives, in the same order.
        """

    def parameter_names(self) -> list[str]:
        """Give a name for each input DataFrame, in the order of the inputs.

        The compiler and the runner put these names in their messages. Clair
        binds the inputs by position, thus a name has no effect on the DAG.
        """
        return []

    def source_text(self) -> str:
        """Give the text that shows how this Trouve makes its rows.

        Discovery keeps this text in ``resolved_transform``, and ``clair
        compile`` writes it to the artifact file. It is the counterpart of the
        SQL of a ``Trouve``.
        """
        return repr(self)

    def source_file(self) -> str | None:
        """Give the file that holds the import statements of the source text.

        ``clair compile`` puts those imports in the artifact file, to make the
        artifact readable on its own. Give None if no such file exists.
        """
        return None
