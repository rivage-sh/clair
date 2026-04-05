"""Column and ColumnType definitions."""

from enum import StrEnum

from pydantic import BaseModel


class ColumnType(StrEnum):
    """Common Snowflake data types.

    Because ColumnType is a StrEnum, members compare equal to their string
    values (e.g. ColumnType.STRING == "STRING"). You can also pass a plain
    string for parameterised types like "NUMBER(18,2)" that don't have a
    named constant.
    """

    STRING = "STRING"
    VARCHAR = "VARCHAR"
    NUMBER = "NUMBER"
    FLOAT = "FLOAT"
    INTEGER = "INTEGER"
    BOOLEAN = "BOOLEAN"
    DATE = "DATE"
    TIMESTAMP = "TIMESTAMP"
    TIMESTAMP_NTZ = "TIMESTAMP_NTZ"
    TIMESTAMP_LTZ = "TIMESTAMP_LTZ"
    TIMESTAMP_TZ = "TIMESTAMP_TZ"
    VARIANT = "VARIANT"
    ARRAY = "ARRAY"
    OBJECT = "OBJECT"


class Column(BaseModel):
    """A column in a Trouve.

    Attributes:
        name: Column name as it appears in Snowflake.
        type: Snowflake data type string (e.g., "STRING", "NUMBER(18,2)").
        docs: Optional documentation for this column.
        nullable: Whether the column allows NULLs.
    """

    name: str
    type: str
    docs: str = ""
    nullable: bool = True
