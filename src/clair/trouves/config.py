"""The default configuration for a database and for a schema."""

from pydantic import BaseModel


class ResolvedConfig(BaseModel):
    """The final warehouse and role configuration of one Trouve.

    Discovery makes this object. It moves up the directory tree and merges the
    profile defaults with __database_config__.py and __schema_config__.py.
    """

    warehouse: str | None = None
    role: str | None = None


class DatabaseDefaults(BaseModel):
    """The defaults for each Trouve in a database directory.

    Set these values in __database_config__.py in the database directory.
    """

    warehouse: str | None = None
    role: str | None = None


class SchemaDefaults(BaseModel):
    """The defaults for each Trouve in a schema directory.

    Set these values in __schema_config__.py in the schema directory. Each value
    that you set replaces the equivalent value in DatabaseDefaults.
    """

    warehouse: str | None = None
    role: str | None = None
