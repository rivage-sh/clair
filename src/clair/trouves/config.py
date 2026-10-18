"""The session configuration of a Trouve: the warehouse, the role, the query tag and the timeout.

Three levels can set it: ``__database_config__.py``, ``__schema_config__.py``,
and the Trouve. Each level replaces the values of the level above it.
"""

from __future__ import annotations

import json

from pydantic import BaseModel, Field, field_validator

from clair._version import __version__

# The keys that clair writes in each query tag. A label must not use one, thus
# a cost report can trust them.
QUERY_TAG_KEYS = frozenset({"app", "clair_version", "run_id", "environment", "trouve"})


class SessionConfig(BaseModel):
    """The session settings that a database, a schema, or a Trouve can set.

    Attributes:
        warehouse: The warehouse that runs the statements of the Trouve.
        role: The role that runs them.
        query_tags: The labels that clair adds to the query tag of each
            statement, for example ``{"team": "finance"}``. A key of a lower
            level replaces the same key of a higher level.
        statement_timeout_seconds: Snowflake cancels a statement that runs
            longer. None keeps the value of the account or the user.
    """

    warehouse: str | None = None
    role: str | None = None
    query_tags: dict[str, str] = {}
    statement_timeout_seconds: int | None = Field(default=None, gt=0)

    @field_validator("query_tags")
    @classmethod
    def _reject_clair_keys(cls, query_tags: dict[str, str]) -> dict[str, str]:
        reserved_keys = sorted(QUERY_TAG_KEYS.intersection(query_tags))
        if reserved_keys:
            raise ValueError(
                f"clair writes the query tag keys {', '.join(reserved_keys)}. Give each label another key."
            )
        return query_tags


class ResolvedConfig(SessionConfig):
    """The final session configuration of one Trouve.

    Discovery makes this object. It starts from the environment defaults, and
    applies __database_config__.py, __schema_config__.py and the Trouve.
    """

    def apply(self, layer: SessionConfig) -> ResolvedConfig:
        """Give a copy in which each value that *layer* sets replaces the value here.

        A blank warehouse or role sets nothing. The labels merge by key.
        """
        resolved = self.model_copy(deep=True)
        if layer.warehouse and layer.warehouse.strip():
            resolved.warehouse = layer.warehouse
        if layer.role and layer.role.strip():
            resolved.role = layer.role
        resolved.query_tags = {**self.query_tags, **layer.query_tags}
        if layer.statement_timeout_seconds is not None:
            resolved.statement_timeout_seconds = layer.statement_timeout_seconds
        return resolved

    def query_tag(self, run_id: str, environment_name: str, logical_address: str) -> str:
        """Give the QUERY_TAG of each statement of the Trouve, as JSON.

        A cost report groups the query history by these keys, and needs no
        configuration.
        """
        tag = {
            "app": "clair",
            "clair_version": __version__,
            "run_id": run_id,
            "environment": environment_name,
            "trouve": logical_address,
            **self.query_tags,
        }
        return json.dumps(tag, sort_keys=True)


class DatabaseDefaults(SessionConfig):
    """The defaults for each Trouve in a database directory.

    Set these values in __database_config__.py in the database directory.
    """


class SchemaDefaults(SessionConfig):
    """The defaults for each Trouve in a schema directory.

    Set these values in __schema_config__.py in the schema directory. Each value
    that you set replaces the equivalent value in DatabaseDefaults.
    """
