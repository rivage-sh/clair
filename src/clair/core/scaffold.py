"""The scaffold. It makes a new Clair project with an example Trouve and a config."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from clair.environments.project_routing import ROUTING_FILE_NAME

# ---------------------------------------------------------------------------
# The file templates.
# ---------------------------------------------------------------------------

_SOURCE_TROUVE_TEMPLATE = '''\
from clair import Trouve, TrouveType

trouve = Trouve(
    type=TrouveType.SOURCE,
)
'''

_ROUTING_TEMPLATE = '''\
"""Clair routing -- gives each environment its physical write target.

Each entry names one environment. The name matches a top-level key in
~/.clair/environments.yml. The route method accepts the logical TrouveAddress
and gives the physical TrouveAddress.

This file starts with one entry, and that entry changes nothing: the physical
address stays equal to the logical address. Change the route method for a
separate target for an environment, for example one database for each person.
See https://clair.rivage.sh/guides/routing/
"""

from enum import StrEnum

from clair import RoutingEntry, RoutingTable, TrouveAddress, TrouveType


class EnvironmentName(StrEnum):
    """The environments of this project.

    Each member matches a top-level key in ~/.clair/environments.yml.
    """

    DEV = "dev"


class DevelopmentRouting(RoutingEntry):
    """Write to the logical address, thus the address stays the same."""

    environment_name: str = EnvironmentName.DEV.value

    def route(
        self, trouve_address: TrouveAddress, trouve_type: TrouveType
    ) -> TrouveAddress:
        return trouve_address


routing = RoutingTable(entries=[DevelopmentRouting()])
'''

_ENVIRONMENTS_TEMPLATE = '''\
# The Clair environments. Each environment has its own connection settings.
# Routing is not here. It lives in the project __routing__.py file.
# Reference: https://github.com/rivage-sh/clair

dev:
  account: your-org-your-account   # for example, myorg-myaccount
  user: your@email.com
  authenticator: externalbrowser   # SSO authentication in a browser
  warehouse: your_warehouse
  threads: 4                       # Trouves that run at one time
  # region: us-east-1              # necessary for the query URLs
  # account_locator: abc12345      # necessary for the query URLs

# A production environment with key pair authentication:
# prod:
#   account: your-org-your-account
#   user: ci_service_user
#   private_key_path: ~/.clair/snowflake_key.p8
#   # private_key_passphrase: your-passphrase   # only if the key is encrypted
#   warehouse: your_warehouse
#   threads: 8
#   region: us-east-1
#   account_locator: abc12345
'''


def _write_if_missing(path: Path, content: str) -> bool:
    """Write *content* to *path*. Make each parent directory that is absent.

    Returns True if the function made the file. Returns False if the file
    already exists.
    """
    if path.exists():
        return False
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)
    return True


def scaffold_project(
    project_dir: Path,
    source_database_name: str,
    source_schema_name: str,
    source_table_name: str,
    home_dir: Path | None = None,
) -> list[tuple[str, str]]:
    """Make a new Clair project in *project_dir*.

    The function writes an example source Trouve file and a project
    ``__routing__.py`` file. It also writes the global
    ``~/.clair/environments.yml`` file, if that file does not exist.

    Args:
        project_dir: The root directory of the new project.
        source_database_name: The name of the source database directory.
        source_schema_name: The name of the source schema directory.
        source_table_name: The name of the source table file.
        home_dir: A replacement for ``Path.home()``. The tests use it.

    Returns:
        A list of ``(status, path)`` tuples. The status is ``"created"`` or
        ``"skipped"``. Each path is a string.
    """
    project_dir = project_dir.resolve()

    # Each project file, as a (relative_path, template_content) pair.
    project_files: list[tuple[str, str]] = [
        (f"{source_database_name}/{source_schema_name}/{source_table_name}.py", _SOURCE_TROUVE_TEMPLATE),
        (ROUTING_FILE_NAME, _ROUTING_TEMPLATE),
    ]

    results: list[tuple[str, str]] = []

    for relative_path, content in project_files:
        full_path = project_dir / relative_path
        created = _write_if_missing(full_path, content)
        status = "created" if created else "skipped"
        results.append((status, str(full_path)))

    # The global environments.yml file in ~/.clair/
    effective_home = home_dir if home_dir is not None else Path.home()
    environments_path = effective_home / ".clair" / "environments.yml"
    created = _write_if_missing(environments_path, _ENVIRONMENTS_TEMPLATE)
    status = "created" if created else "skipped"
    results.append((status, str(environments_path)))

    return results


def write_environments_yml(
    env_data: dict[str, Any],
    env_name: str = "dev",
    *,
    home_dir: Path | None = None,
) -> Path:
    """Write an environments.yml file from the data that the user gave.

    Args:
        env_data: The key and value pairs of the environment, such as the
            account and the user.
        env_name: The name of the environment section.
        home_dir: A replacement for ``Path.home()``. The tests use it.

    Returns:
        The path of the new environments.yml file.
    """
    effective_home = home_dir if home_dir is not None else Path.home()
    environments_path = effective_home / ".clair" / "environments.yml"

    lines = [
        "# The Clair environments. Each environment has its own connection settings.",
        "",
        f"{env_name}:",
    ]
    for key, value in env_data.items():
        lines.append(f"  {key}: {value}")

    lines.append("")

    environments_path.parent.mkdir(parents=True, exist_ok=True)
    environments_path.write_text("\n".join(lines))
    return environments_path
