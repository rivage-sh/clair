"""Environment loading from ~/.clair/environments.py."""

from __future__ import annotations

import importlib.util
import os
from collections.abc import Callable
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict

from clair.environments.routing import RoutingConfig
from clair.exceptions import (
    EnvironmentNotFoundError,
    EnvironmentsFileNotFoundError,
    InvalidEnvironmentsFileError,
)

DEFAULT_ENVIRONMENTS_PATH = Path.home() / ".clair" / "environments.py"


class Environment(BaseModel):
    """A single environment from environments.py."""

    model_config = ConfigDict(populate_by_name=True, arbitrary_types_allowed=True)

    # Identity
    name: str = ""

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

    # Routing: a RoutingConfig subclass or any callable (db, schema, table) -> "db.schema.table"
    routing: RoutingConfig | Callable[[str, str, str], str] | None = None

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


def _load_environments_module(path: Path) -> dict[str, Environment]:
    """Load and execute an environments.py file, returning its environments dict."""
    spec = importlib.util.spec_from_file_location("_clair_environments", path)
    if spec is None or spec.loader is None:
        raise EnvironmentsFileNotFoundError(str(path))

    module = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(module)  # type: ignore[union-attr]
    except Exception as exc:
        raise InvalidEnvironmentsFileError(str(path), str(exc)) from exc

    if not hasattr(module, "environments"):
        raise InvalidEnvironmentsFileError(
            str(path), "file must define an 'environments' dict"
        )

    environments = module.environments
    if not isinstance(environments, dict):
        raise InvalidEnvironmentsFileError(
            str(path), f"'environments' must be a dict, got {type(environments).__name__}"
        )

    return environments


def load_environment(
    env_name: str | None = None,
    environments_path: Path | None = None,
) -> tuple[str, Environment]:
    """Load an environment from environments.py.

    Resolution order for env name:
    1. env_name argument
    2. CLAIR_ENV environment variable
    3. "dev"

    Args:
        env_name: Explicit environment name.
        environments_path: Path to environments.py. Defaults to ~/.clair/environments.py.

    Returns:
        Tuple of (resolved_env_name, Environment).

    Raises:
        EnvironmentsFileNotFoundError: If environments.py does not exist.
        InvalidEnvironmentsFileError: If environments.py is malformed.
        EnvironmentNotFoundError: If the requested environment is not defined.
    """
    resolved_name = env_name or os.environ.get("CLAIR_ENV") or "dev"
    path = environments_path or DEFAULT_ENVIRONMENTS_PATH

    if not path.exists():
        raise EnvironmentsFileNotFoundError(str(path))

    environments = _load_environments_module(path)

    if resolved_name not in environments:
        raise EnvironmentNotFoundError(resolved_name, [str(k) for k in environments])

    environment = environments[resolved_name]
    if not isinstance(environment, Environment):
        raise InvalidEnvironmentsFileError(
            str(path),
            f"environments['{resolved_name}'] must be an Environment instance, "
            f"got {type(environment).__name__}",
        )

    environment.name = resolved_name
    return resolved_name, environment
