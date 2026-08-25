"""The Snowflake connection settings of the integration tests.

Only the account and the private key come from a secret. The user, the role and
the warehouse are names inside the account: `tests/integration/scripts/clair_pr_testing_setup.sql`
makes them, and that file is in the repository.
"""

from __future__ import annotations

import getpass
import os
import re
from dataclasses import dataclass
from pathlib import Path

DATABASE_NAME = "clair_pr_testing"

DEFAULT_USER = "clair_pr_testing_user"
DEFAULT_ROLE = "clair_pr_testing_f"
DEFAULT_WAREHOUSE = "clair_pr_testing_wh"

ENVIRONMENT_NAME = "pr_testing"

# A schema name becomes a Snowflake identifier, thus it accepts letters,
# digits and underscores only. Snowflake is not case sensitive, and clair uses
# lower case names.
SCHEMA_NAME_PATTERN = re.compile(r"^[a-z][a-z0-9_]{0,40}$")

# The golden schemas hold the source tables. A run clones them, and it never
# writes to them.
GOLDEN_SCHEMA_NAMES = frozenset({"example_1", "example_2", "example_3", "example_4"})
PROTECTED_SCHEMA_NAMES = GOLDEN_SCHEMA_NAMES | {"public", "information_schema"}


class IntegrationConfigError(RuntimeError):
    """The environment does not hold the Snowflake settings."""


@dataclass(frozen=True)
class IntegrationConfig:
    """Everything that one integration test run needs."""

    account: str
    user: str
    role: str
    warehouse: str
    schema_name: str
    private_key_path: str | None
    private_key_passphrase: str | None
    password: str | None

    def to_connection_dict(self) -> dict[str, str | None]:
        """Give the dict that SnowflakeAdapter.connect() accepts."""
        connection: dict[str, str | None] = {
            "account": self.account,
            "user": self.user,
            "role": self.role,
            "warehouse": self.warehouse,
        }
        if self.private_key_path:
            connection["private_key_path"] = self.private_key_path
            if self.private_key_passphrase:
                connection["private_key_passphrase"] = self.private_key_passphrase
        else:
            connection["password"] = self.password
        return connection

    def to_environments_yaml(self) -> str:
        """Give the content of a ~/.clair/environments.yml for this run."""
        lines = [
            f"{ENVIRONMENT_NAME}:",
            f"  account: {self.account}",
            f"  user: {self.user}",
            f"  role: {self.role}",
            f"  warehouse: {self.warehouse}",
        ]
        if self.private_key_path:
            lines.append(f"  private_key_path: {self.private_key_path}")
            if self.private_key_passphrase:
                lines.append(
                    f"  private_key_passphrase: {self.private_key_passphrase}"
                )
        else:
            lines.append(f"  password: {self.password}")
        return "\n".join(lines) + "\n"


def normalise_schema_name(name: str) -> str:
    """Give the lower case schema name, and refuse an unusual character.

    Cleanup drops a schema by this name. A name that holds a quotation mark or a
    space must never reach a DROP statement.
    """
    candidate = name.strip().lower()
    if not SCHEMA_NAME_PATTERN.match(candidate):
        raise IntegrationConfigError(
            f"The schema name {name!r} is not valid. Use a letter first, then "
            f"letters, digits or underscores, 41 characters maximum."
        )
    if candidate in PROTECTED_SCHEMA_NAMES:
        raise IntegrationConfigError(
            f"The schema name {candidate} is protected. A run must not write to it."
        )
    return candidate


def default_schema_name() -> str:
    """Give the schema name for a run that names none.

    GitHub Actions sets the prefix. On a workstation the name holds the user
    name and the process id, thus two runs never collide.
    """
    try:
        user_name = getpass.getuser()
    except (OSError, KeyError):
        user_name = "unknown"
    cleaned = re.sub(r"[^A-Za-z0-9]", "", user_name).lower() or "unknown"
    return f"local_{cleaned}_{os.getpid()}"


def load_config() -> IntegrationConfig:
    """Read the settings from the environment.

    Raises:
        IntegrationConfigError: If the account or the credentials are absent.
    """
    account = os.environ.get("CLAIR_PR_TESTING_SNOWFLAKE_ACCOUNT", "").strip()
    if not account:
        raise IntegrationConfigError(
            "CLAIR_PR_TESTING_SNOWFLAKE_ACCOUNT is empty. The integration tests need a "
            "Snowflake account. See tests/integration/README.md."
        )

    private_key_path = os.environ.get("CLAIR_PR_TESTING_SNOWFLAKE_PRIVATE_KEY_PATH", "").strip()
    password = os.environ.get("CLAIR_PR_TESTING_SNOWFLAKE_PASSWORD", "").strip()
    if not private_key_path and not password:
        raise IntegrationConfigError(
            "The integration tests need CLAIR_PR_TESTING_SNOWFLAKE_PRIVATE_KEY_PATH or "
            "CLAIR_PR_TESTING_SNOWFLAKE_PASSWORD. See tests/integration/README.md."
        )
    if private_key_path and not Path(private_key_path).is_file():
        raise IntegrationConfigError(
            f"The private key file {private_key_path} does not exist."
        )

    schema_name = os.environ.get("CLAIR_PR_TESTING_SCHEMA_NAME", "").strip()
    return IntegrationConfig(
        account=account,
        user=os.environ.get("CLAIR_PR_TESTING_SNOWFLAKE_USER", DEFAULT_USER).strip(),
        role=os.environ.get("CLAIR_PR_TESTING_SNOWFLAKE_ROLE", DEFAULT_ROLE).strip(),
        warehouse=os.environ.get(
            "CLAIR_PR_TESTING_SNOWFLAKE_WAREHOUSE", DEFAULT_WAREHOUSE
        ).strip(),
        schema_name=normalise_schema_name(schema_name or default_schema_name()),
        private_key_path=private_key_path or None,
        private_key_passphrase=os.environ.get(
            "CLAIR_PR_TESTING_SNOWFLAKE_PRIVATE_KEY_PASSPHRASE"
        )
        or None,
        password=password or None,
    )
