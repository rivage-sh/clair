"""Clair reads the environments from ~/.clair/environments.yml."""

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
    """One environment from environments.yml."""

    model_config = ConfigDict(populate_by_name=True)

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

    # The routing policy.
    routing: Routing | None = None

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


def _validate_routing_block(routing_raw: dict[str, Any]) -> None:
    """Examine the routing block before Pydantic reads it.

    This function finds an absent policy value and an unknown policy value. Then
    it raises a clair error type that the CLI already knows.
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
        InvalidRoutingPolicyError: If the file names an unknown routing policy.
        InvalidRoutingConfigError: If the routing block has a bad structure.
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
        # Show each Pydantic error as a clair error that the CLI already knows.
        # One example is an absent schema_name for the schema_isolation policy.
        raise InvalidRoutingConfigError(str(exc)) from exc
