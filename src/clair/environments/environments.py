"""Clair reads the environments from ~/.clair/environments.yml."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from clair.exceptions import (
    EnvironmentNotFoundError,
    EnvironmentsFileNotFoundError,
    InvalidEnvironmentError,
)

DEFAULT_ENVIRONMENTS_PATH = Path.home() / ".clair" / "environments.yml"

# The number of Trouves that clair runs at one time, if the environment and
# the command line give no other value.
DEFAULT_THREADS = 4


def _first_error_message(exc: ValidationError) -> str:
    """Take the first message out of a Pydantic error.

    Pydantic prints a report of many lines. A CLI message needs one sentence.
    """
    errors = exc.errors()
    if not errors:
        return str(exc)
    first = errors[0]
    key_name = ".".join(str(part) for part in first.get("loc", ()))
    message = str(first.get("msg", ""))
    return f"'{key_name}': {message}" if key_name else message


class Environment(BaseModel):
    """One environment from environments.yml.

    An environment holds connection settings only. Routing lives in the project
    ``__routing__.py``, under the same environment name.
    """

    # "forbid" makes an unknown key an error. A leftover routing block, or a
    # misspelt key, would otherwise disappear without a word.
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    # The identity of the environment.
    name: str

    # The connection fields. Each one is mandatory.
    account: str
    user: str
    warehouse: str

    # The authentication fields. Clair uses one group when it connects.
    authenticator: str | None = None
    password: str | None = None
    private_key_path: str | None = None
    private_key_passphrase: str | None = None

    # The optional connection fields.
    role: str | None = None
    region: str | None = None
    account_locator: str | None = None

    # The number of Trouves that clair runs at one time. Each thread holds a
    # private warehouse connection, so this is also the connection count.
    # `clair run --threads` and `clair test --threads` replace this value.
    threads: int = Field(default=DEFAULT_THREADS, ge=1)

    def to_connection_dict(self) -> dict[str, Any]:
        """Give the connection dict that SnowflakeAdapter.connect() needs."""
        d: dict[str, Any] = {
            "account": self.account,
            "user": self.user,
            "warehouse": self.warehouse,
            "role": self.role,
            "region": self.region,
            "account_locator": self.account_locator,
        }
        if self.authenticator:
            d["authenticator"] = self.authenticator
        if self.password:
            d["password"] = self.password
        if self.private_key_path:
            d["private_key_path"] = self.private_key_path
        if self.private_key_passphrase:
            d["private_key_passphrase"] = self.private_key_passphrase
        return d


def resolve_env_name(env_name: str | None = None) -> str:
    """Give the environment name: the argument, then CLAIR_ENV, then "dev"."""
    return env_name or os.environ.get("CLAIR_ENV") or "dev"


def load_environment(
    env_name: str | None = None,
    environments_path: Path | None = None,
) -> tuple[str, Environment]:
    """Load one environment from environments.yml.

    The function looks for the environment name in this order:
    1. The env_name argument
    2. The CLAIR_ENV environment variable
    3. The name "dev"

    Args:
        env_name: The environment name that you select.
        environments_path: The path to environments.yml. The default path is
            ~/.clair/environments.yml.

    Returns:
        A tuple of (resolved_env_name, Environment).

    Raises:
        EnvironmentsFileNotFoundError: If environments.yml does not exist.
        EnvironmentNotFoundError: If environments.yml has no such environment.
        InvalidEnvironmentError: If the environment block holds an unknown key.
    """
    resolved_name = resolve_env_name(env_name)
    path = environments_path or DEFAULT_ENVIRONMENTS_PATH

    if not path.exists():
        raise EnvironmentsFileNotFoundError(str(path))

    with open(path) as f:
        raw = yaml.safe_load(f)

    if not isinstance(raw, dict):
        raise EnvironmentsFileNotFoundError(str(path))

    if resolved_name not in raw:
        raise EnvironmentNotFoundError(resolved_name, [str(k) for k in raw])

    env_data: dict[str, Any] = raw[resolved_name]

    try:
        return resolved_name, Environment(name=resolved_name, **env_data)
    except ValidationError as exc:
        # An unknown key is almost always a typo, or a routing block that the user
        # did not move to __routing__.py. Both send writes to the wrong target.
        raise InvalidEnvironmentError(
            resolved_name, str(path), _first_error_message(exc)
        ) from exc
