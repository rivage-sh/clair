"""PandasTrouve -- a Trouve that a Python function materializes.

Clair reads each upstream Trouve from Snowflake into a DataFrame, gives the
DataFrames to your function, and writes the result back to Snowflake.
"""

from __future__ import annotations

import inspect
from collections.abc import Callable

import pandas as pd
from pydantic import Field, model_validator

from clair.trouves.run_config import RunMode
from clair.trouves.trouve import ExecutionType, TrouveAbc, TrouveType

_VARIADIC_KINDS = (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD)


class PandasTrouve(TrouveAbc):
    """A Trouve that a pandas function materializes.

    Clair binds ``inputs`` to the parameters of ``transform`` by position. Thus
    the first Trouve in ``inputs`` becomes the first parameter, the second
    becomes the second parameter, and so on. Your function receives plain
    DataFrames, so you can call it directly in a test or in a notebook.

    Example:
        >>> def daily_counts(events: pd.DataFrame) -> pd.DataFrame:
        ...     return events.groupby("day", as_index=False).agg(n=("day", "size"))
        >>> trouve = PandasTrouve(transform=daily_counts, inputs=[events_trouve])

    Attributes:
        transform: The function that gives the output DataFrame.
        inputs: The upstream Trouves, in the parameter order of ``transform``.

    ``TrouveAbc`` holds the attributes that every backend shares.
    """

    transform: Callable[..., pd.DataFrame] = Field(exclude=True)
    inputs: list[TrouveAbc] = Field(default_factory=list, exclude=True)

    @property
    def execution_type(self) -> ExecutionType:
        return ExecutionType.PANDAS

    def upstream_trouves(self) -> list[TrouveAbc]:
        """Give the upstream Trouves, in the parameter order of the transform."""
        return list(self.inputs)

    def parameter_names(self) -> list[str]:
        """Give the parameter names of the transform, in order.

        The runner and the compiler use these names in their messages. Clair
        binds the inputs by position, thus a name has no effect on the DAG.
        """
        return list(inspect.signature(self.transform).parameters)

    @model_validator(mode="after")
    def _validate_transform(self) -> PandasTrouve:
        if self.type != TrouveType.TABLE:
            raise ValueError(
                f"PandasTrouve must be TABLE type, got '{self.type.value}'"
            )
        if self.run_config.run_mode == RunMode.INCREMENTAL:
            raise ValueError("PandasTrouve does not support incremental mode")

        parameters = list(inspect.signature(self.transform).parameters.values())
        variadic = [p.name for p in parameters if p.kind in _VARIADIC_KINDS]
        if variadic:
            raise ValueError(
                f"transform must not have *args or **kwargs, found: {', '.join(variadic)}. "
                "Clair binds each input to a named parameter, by position."
            )
        if len(parameters) != len(self.inputs):
            parameter_names = ", ".join(p.name for p in parameters) or "(none)"
            raise ValueError(
                f"transform takes {len(parameters)} parameter(s) but inputs has "
                f"{len(self.inputs)} Trouve(s). Clair binds them by position. "
                f"Parameters: {parameter_names}"
            )
        return self

