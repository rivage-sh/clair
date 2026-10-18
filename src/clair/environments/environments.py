"""Clair reads the environments from ~/.clair/environments.yml."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Annotated, Any

import yaml
from pydantic import BaseModel, ConfigDict, Discriminator, Field, Tag, ValidationError

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
    # A misspelt key gives a missing key and an unknown key. The unknown key
    # names the text that the user wrote, thus it comes first.
    errors = sorted(errors, key=lambda error: error["type"] != "extra_forbidden")
    first = errors[0]
    key_name = ".".join(str(part) for part in first.get("loc", ()))
    message = str(first.get("msg", ""))
    return f"'{key_name}': {message}" if key_name else message


class _SettingsModel(BaseModel):
    # "forbid" makes an unknown key an error. A misspelt key, or a key of a
    # different authentication method, would otherwise disappear without a word.
    model_config = ConfigDict(extra="forbid", frozen=True)


class Connection(_SettingsModel):
    """The Snowflake account, and the session defaults of each connection.

    Attributes:
        account: The account identifier, for example ``myorg-myaccount``.
        user: The Snowflake user.
        warehouse: The warehouse of the session.
        role: The role of the session. None uses the default role of the user.
        region: The region of the account. The query URLs need it.
        account_locator: The account locator. The query URLs need it.
    """

    account: str
    user: str
    warehouse: str
    role: str | None = None
    region: str | None = None
    account_locator: str | None = None


class Password(_SettingsModel):
    """Log in with the password of the user."""

    password: str


class PrivateKey(_SettingsModel):
    """Log in with a key pair.

    Attributes:
        private_key_path: The path of the private key file, in PEM format.
        private_key_passphrase: The passphrase of an encrypted key, or None.
    """

    private_key_path: str
    private_key_passphrase: str | None = None


class Authenticator(_SettingsModel):
    """Log in with a Snowflake authenticator.

    Attributes:
        authenticator: The value that the Snowflake connector reads, for example
            ``externalbrowser`` for SSO in a browser.
    """

    authenticator: str


# The key that names each authentication method in environments.yml. A block
# that holds two of these keys goes to the first method, and the extra key of
# the other method is an error.
_AUTHENTICATION_KEYS = {
    "password": "password",
    "private_key_path": "private_key",
    "authenticator": "authenticator",
}
_AUTHENTICATION_TAGS = {
    Password: "password",
    PrivateKey: "private_key",
    Authenticator: "authenticator",
}


def _authentication_tag(value: Any) -> str | None:
    """Give the tag of the authentication method that a block or an object holds."""
    if isinstance(value, dict):
        for key, tag in _AUTHENTICATION_KEYS.items():
            if key in value:
                return tag
        return None
    return _AUTHENTICATION_TAGS.get(type(value))


Authentication = Annotated[
    Annotated[Password, Tag("password")]
    | Annotated[PrivateKey, Tag("private_key")]
    | Annotated[Authenticator, Tag("authenticator")],
    Discriminator(
        _authentication_tag,
        custom_error_type="authentication_method",
        custom_error_message="give one of password, private_key_path or authenticator",
    ),
]


class Environment(_SettingsModel):
    """One environment from environments.yml.

    An environment holds connection settings only. Routing lives in the project
    ``__routing__.py``, under the same environment name.

    Attributes:
        name: The environment name. It matches a top-level key in
            environments.yml, and an entry in ``__routing__.py``.
        connection: The account, and the session defaults.
        authentication: One method: Password, PrivateKey, or Authenticator.
        threads: The number of Trouves that clair runs at one time. Each thread
            holds a private warehouse connection, so this is also the connection
            count. ``clair run --threads`` and ``clair test --threads`` replace it.
    """

    name: str
    connection: Connection
    authentication: Authentication
    threads: int = Field(default=DEFAULT_THREADS, ge=1)


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
