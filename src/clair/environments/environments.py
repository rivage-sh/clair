"""Environment loading from ~/.clair/environments.yml."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, ConfigDict

from clair.exceptions import (
    EnvironmentNotFoundError,
    EnvironmentsFileNotFoundError,
    RoutingInEnvironmentsFileError,
)

DEFAULT_ENVIRONMENTS_PATH = Path.home() / ".clair" / "environments.yml"


class Environment(BaseModel):
    """A single environment from environments.yml.

    An environment holds connection settings only. Routing lives in the project
    ``__routing__.py``, under the same environment name.
    """

    model_config = ConfigDict(populate_by_name=True)

    # Identity
    name: str

    # Connection (required)
    account: str
    user: str
    warehouse: str

    # Auth (one group used at connect time)
    authenticator: str | None = None
    password: str | None = None
    private_key_path: str | None = None
    private_key_passphrase: str | None = None

    # Optional connection
    role: str | None = None
    region: str | None = None
    account_locator: str | None = None

    def to_connection_dict(self) -> dict[str, Any]:
        """Return the connection dict expected by SnowflakeAdapter.connect()."""
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


def load_environment(
    env_name: str | None = None,
    environments_path: Path | None = None,
) -> tuple[str, Environment]:
    """Load an environment from environments.yml.

    Resolution order for env name:
    1. env_name argument
    2. CLAIR_ENV environment variable
    3. "dev"

    Args:
        env_name: Explicit environment name.
        environments_path: Path to environments.yml. Defaults to ~/.clair/environments.yml.

    Returns:
        Tuple of (resolved_env_name, Environment).

    Raises:
        EnvironmentsFileNotFoundError: If environments.yml does not exist.
        EnvironmentNotFoundError: If the requested environment is not in environments.yml.
        RoutingInEnvironmentsFileError: If the environment still has a routing block.
    """
    resolved_name = env_name or os.environ.get("CLAIR_ENV") or "dev"
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

    # Pydantic drops an unknown key without a word. A leftover routing block
    # would then send every write to the production names.
    if "routing" in env_data:
        raise RoutingInEnvironmentsFileError(str(path), resolved_name)

    return resolved_name, Environment(name=resolved_name, **env_data)
