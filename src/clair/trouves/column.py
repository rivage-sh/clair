"""The Column and ColumnType definitions."""

from enum import StrEnum

from pydantic import BaseModel


class ColumnType(StrEnum):
    """The usual Snowflake data types.

    ColumnType is a StrEnum. Thus each member is equal to its own string, for
    example ColumnType.STRING == "STRING". For a type with parameters, such as
    "NUMBER(18,2)", give a plain string, because this enum has no such member.
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
        name: The column name, as Snowflake shows it.
        type: The Snowflake data type, for example "STRING" or "NUMBER(18,2)".
        docs: Optional documentation text for this column.
        nullable: True if the column accepts a NULL value.
    """

    name: str
    type: str
    docs: str = ""
    nullable: bool = True
