"""TrouveAddress -- the address of one Trouve in the warehouse.

An address holds a database name, a schema name, and a table name. It validates
each name when you make it, so an address that exists is a valid one.

Clair gives each Trouve three addresses:

* The logical address, which the file path gives.
* The physical address, which routing makes from the logical address.
* The staging address, which adds the run-scoped suffix to the physical address.

At this time the validator applies the Snowflake identifier rules.
"""

from __future__ import annotations

import re

from pydantic import BaseModel, ConfigDict, ValidationError, field_validator

from clair.exceptions import InvalidTrouveAddressError

# Snowflake accepts these characters in an unquoted identifier.
_VALID_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")

# The maximum identifier length of Snowflake.
#
# A test against a live account showed that the limit applies to each name, and
# not to the full database.schema.table path. Snowflake accepts a table name of
# 255 characters. It rejects 256 characters with the message "Object name '...'
# exceeds maximum length limit of 255 characters". It also accepts a path of 767
# characters, which is 255 characters for each of the three names. Therefore this
# validator applies the limit to each name on its own.
_MAX_IDENTIFIER_LENGTH = 255


def first_error_message(exc: ValidationError) -> str:
    """Take the first message out of a Pydantic error.

    Pydantic prints a report of many lines. A CLI message needs one sentence.
    """
    errors = exc.errors()
    if not errors:
        return str(exc)
    first = errors[0]
    message = str(first.get("msg", "")).removeprefix("Value error, ")
    location = ".".join(str(part) for part in first.get("loc", ()))
    return f"{location}: {message}" if location else message


class TrouveAddress(BaseModel):
    """The full address of one Trouve in the warehouse.

    The model is frozen, so an address is hashable and safe to share. To make a
    changed copy, call ``model_copy(update={...})``.
    """

    model_config = ConfigDict(frozen=True)

    database_name: str
    schema_name: str
    table_name: str

    @field_validator("database_name", "schema_name", "table_name")
    @classmethod
    def _validate_identifier(cls, value: str) -> str:
        """Reject a name that Snowflake cannot use as an unquoted identifier."""
        if len(value) > _MAX_IDENTIFIER_LENGTH:
            raise ValueError(
                f"'{value}' has {len(value)} characters. "
                f"The maximum is {_MAX_IDENTIFIER_LENGTH}."
            )
        if not _VALID_IDENTIFIER.match(value):
            raise ValueError(
                f"'{value}' is not a valid identifier. An identifier starts with a "
                "letter or an underscore. The other characters are letters, digits, "
                "underscores or dollar signs."
            )
        return value

    @classmethod
    def parse(cls, dotted_address: str) -> TrouveAddress:
        """Make an address from a "database_name.schema_name.table_name" string.

        Args:
            dotted_address: The address as a string.

        Returns:
            The validated address.

        Raises:
            InvalidTrouveAddressError: If the string is not a valid address.
        """
        parts = dotted_address.split(".")
        if len(parts) != 3:
            raise InvalidTrouveAddressError(
                dotted_address,
                f"an address needs 3 dot-separated parts, but this name has "
                f"{len(parts)}",
            )
        try:
            return cls(
                database_name=parts[0], schema_name=parts[1], table_name=parts[2]
            )
        except ValidationError as exc:
            raise InvalidTrouveAddressError(
                dotted_address, first_error_message(exc)
            ) from exc

    def __str__(self) -> str:
        return f"{self.database_name}.{self.schema_name}.{self.table_name}"
