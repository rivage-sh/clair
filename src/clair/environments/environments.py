"""Environment loading from ~/.clair/environments.yml."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, ConfigDict, ValidationError

from clair.environments.routing import Routing
from clair.exceptions import (
    EnvironmentNotFoundError,
    EnvironmentsFileNotFoundError,
    InvalidRoutingConfigError,
    InvalidRoutingPolicyError,
)

DEFAULT_ENVIRONMENTS_PATH = Path.home() / ".clair" / "environments.yml"


class Environment(BaseModel):
    """A single environment from environments.yml."""

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

    # Routing
    routing: Routing | None = None

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


def _validate_routing_block(routing_raw: dict[str, Any]) -> None:
    """Pre-validate routing block before Pydantic parses it.

    Catches missing/unknown policy values and re-raises as clair-specific
    error types that the CLI already handles.
    """
    if "policy" not in routing_raw:
        raise InvalidRoutingConfigError("routing block requires 'policy'")

    policy = routing_raw["policy"]
    valid_policies = {"database_override", "schema_isolation"}
    if policy not in valid_policies:
        raise InvalidRoutingPolicyError(policy)


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
        InvalidRoutingPolicyError: If an unknown routing policy is specified.
        InvalidRoutingConfigError: If the routing block is malformed.
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

    routing_raw = env_data.get("routing")
    if isinstance(routing_raw, dict):
        _validate_routing_block(routing_raw)

    try:
        environment = Environment(name=resolved_name, **env_data)
        return resolved_name, environment
    except ValidationError as exc:
        # Surface Pydantic validation errors (e.g. missing schema_name for
        # schema_isolation) as clair-specific errors the CLI already catches.
        raise InvalidRoutingConfigError(str(exc)) from exc
