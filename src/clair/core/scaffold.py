"""The scaffold. It makes a new Clair project with an example Trouve and a config."""

from __future__ import annotations

import re
from pathlib import Path

import yaml

from clair.environments.environments import Environment
from clair.environments.project_routing import ROUTING_FILE_NAME

# ---------------------------------------------------------------------------
# The file templates.
# ---------------------------------------------------------------------------

_SOURCE_TROUVE_TEMPLATE = '''\
from clair import Source

trouve = Source()
'''

_ROUTING_TEMPLATE = '''\
"""Clair routing -- gives each environment its physical write target.

This file also marks the root of the clair project. Each clair command walks up
from the working directory to this file, in the same way that git finds .git.
Keep the file at the project root, also when it holds no entry.

Each entry names one environment. The name matches a top-level key in
~/.clair/environments.yml. The route method accepts the logical TrouveAddress
and gives the physical TrouveAddress.

This file starts with one entry, and that entry changes nothing: the physical
address stays equal to the logical address, which is the production address.
Change the route method for a separate target for an environment, for example
one database for each person.
See https://rivage-sh.github.io/clair/topics/routing/
"""

from enum import StrEnum

from clair import RoutingEntry, RoutingTable, TrouveAddress, TrouveType


class EnvironmentName(StrEnum):
    """The environments of this project.

    Each member matches a top-level key in ~/.clair/environments.yml.
    """

    {member_name} = "{env_name}"


class PassthroughRouting(RoutingEntry):
    """Write to the logical address, thus the address stays the same."""

    environment_name: str = EnvironmentName.{member_name}.value

    def route(
        self, trouve_address: TrouveAddress, trouve_type: TrouveType
    ) -> TrouveAddress:
        return trouve_address


routing = RoutingTable(entries=[PassthroughRouting()])
'''

_ENVIRONMENTS_TEMPLATE = '''\
# The Clair environments. Each environment has its own connection settings.
# Routing is not here. It lives in the project __routing__.py file.
# Reference: https://rivage-sh.github.io/clair/topics/environments/

{env_name}:
  connection:
    account: your-org-your-account   # for example, myorg-myaccount
    user: your@email.com
    warehouse: your_warehouse
    # role: your_role                # the default role of the user, if you omit it
    # region: us-east-1              # necessary for the query URLs
    # account_locator: abc12345      # necessary for the query URLs
  authentication:
    authenticator: externalbrowser   # SSO authentication in a browser
  threads: 4                         # Trouves that run at one time

# A production environment with key pair authentication:
# prod:
#   connection:
#     account: your-org-your-account
#     user: ci_service_user
#     warehouse: your_warehouse
#     region: us-east-1
#     account_locator: abc12345
#   authentication:
#     private_key_path: ~/.clair/snowflake_key.p8
#     # private_key_passphrase: your-passphrase   # only if the key is encrypted
#   threads: 8
'''


def _enum_member_name(env_name: str) -> str:
    """Give the StrEnum member name of *env_name*, for example ``DEV`` for ``dev``."""
    member_name = re.sub(r"\W", "_", env_name).upper()
    if not member_name[:1].isalpha():
        member_name = f"ENV_{member_name}"
    return member_name


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
    env_name: str,
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
        env_name: The environment of the routing entry. It is a top-level key
            of environments.yml.
        home_dir: A replacement for ``Path.home()``. The tests use it.

    Returns:
        A list of ``(status, path)`` tuples. The status is ``"created"`` or
        ``"skipped"``. Each path is a string.
    """
    project_dir = project_dir.resolve()

    # Each project file, as a (relative_path, template_content) pair.
    project_files: list[tuple[str, str]] = [
        (f"{source_database_name}/{source_schema_name}/{source_table_name}.py", _SOURCE_TROUVE_TEMPLATE),
        (
            ROUTING_FILE_NAME,
            _ROUTING_TEMPLATE.format(
                member_name=_enum_member_name(env_name), env_name=env_name
            ),
        ),
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
    created = _write_if_missing(
        environments_path, _ENVIRONMENTS_TEMPLATE.format(env_name=env_name)
    )
    status = "created" if created else "skipped"
    results.append((status, str(environments_path)))

    return results


def write_environments_yml(
    environment: Environment,
    *,
    home_dir: Path | None = None,
) -> Path:
    """Write an environments.yml file that holds one environment.

    Args:
        environment: The environment to write. Its name becomes the top-level key.
        home_dir: A replacement for ``Path.home()``. The tests use it.

    Returns:
        The path of the new environments.yml file.
    """
    effective_home = home_dir if home_dir is not None else Path.home()
    environments_path = effective_home / ".clair" / "environments.yml"

    settings = environment.model_dump(exclude={"name"}, exclude_none=True)
    header = "# The Clair environments. Each environment has its own connection settings.\n\n"
    body = yaml.safe_dump({environment.name: settings}, sort_keys=False)

    environments_path.parent.mkdir(parents=True, exist_ok=True)
    environments_path.write_text(header + body)
    return environments_path
