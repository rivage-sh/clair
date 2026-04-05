"""Database and schema-level configuration defaults."""

from pydantic import BaseModel


class ResolvedConfig(BaseModel):
    """Merged warehouse/role configuration for a single Trouve.

    Built by discovery by walking up the directory tree and merging
    profile defaults with __database_config__.py and __schema_config__.py.
    """

    warehouse: str | None = None
    role: str | None = None


class DatabaseDefaults(BaseModel):
    """Defaults for all Trouves within a database directory.

    Defined in __database_config__.py at the database directory level.
    """

    warehouse: str | None = None
    role: str | None = None


class SchemaDefaults(BaseModel):
    """Defaults for all Trouves within a schema directory.

    Defined in __schema_config__.py at the schema directory level.
    Overrides DatabaseDefaults for fields that are set.
    """

    warehouse: str | None = None
    role: str | None = None
