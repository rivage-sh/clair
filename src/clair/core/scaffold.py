"""Scaffold -- create a new Clair project with example Trouves and config."""

from __future__ import annotations

from pathlib import Path

# ---------------------------------------------------------------------------
# File templates
# ---------------------------------------------------------------------------

_SOURCE_TROUVE_TEMPLATE = '''\
from clair import Trouve, TrouveType

trouve = Trouve(
    type=TrouveType.SOURCE,
)
'''

_ENVIRONMENTS_TEMPLATE = '''\
# Clair environments — connection settings per environment.
# Reference: https://github.com/your-org/clair

dev:
  account: your-org-your-account   # e.g. myorg-myaccount
  user: your@email.com
  authenticator: externalbrowser   # SSO login via browser
  warehouse: your_warehouse
  # region: us-east-1              # required for query URLs
  # account_locator: abc12345      # required for query URLs

# Production environment (key-pair auth):
# prod:
#   account: your-org-your-account
#   user: ci_service_user
#   private_key_path: ~/.clair/snowflake_key.p8
#   # private_key_passphrase: your-passphrase   # only if key is encrypted
#   warehouse: your_warehouse
#   region: us-east-1
#   account_locator: abc12345
'''


def _write_if_missing(path: Path, content: str) -> bool:
    """Write *content* to *path*, creating parent dirs as needed.

    Returns True if the file was created, False if it already existed.
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
    """Create a new Clair project at *project_dir*.

    Generates an example source Trouve file and a global
    ``~/.clair/environments.yml`` (if it does not already exist).

    Args:
        project_dir: Root directory for the new project.
        source_database_name: Name for the source database directory.
        source_schema_name: Name for the source schema directory.
        source_table_name: Name for the source table file.
        home_dir: Override for ``Path.home()`` (used in tests).

    Returns:
        List of ``(status, path)`` tuples where status is ``"created"`` or
        ``"skipped"``.  Each path is a string.
    """
    project_dir = project_dir.resolve()

    # All project files: (relative_path, template_content)
    project_files: list[tuple[str, str]] = [
        (f"{source_database_name}/{source_schema_name}/{source_table_name}.py", _SOURCE_TROUVE_TEMPLATE),
    ]

    results: list[tuple[str, str]] = []

    for relative_path, content in project_files:
        full_path = project_dir / relative_path
        created = _write_if_missing(full_path, content)
        status = "created" if created else "skipped"
        results.append((status, str(full_path)))

    # Global environments.yml in ~/.clair/
    effective_home = home_dir if home_dir is not None else Path.home()
    environments_path = effective_home / ".clair" / "environments.yml"
    created = _write_if_missing(environments_path, _ENVIRONMENTS_TEMPLATE)
    status = "created" if created else "skipped"
    results.append((status, str(environments_path)))

    return results


def write_environments_yml(
    env_data: dict[str, str],
    env_name: str = "dev",
    *,
    home_dir: Path | None = None,
) -> Path:
    """Write an environments.yml file from interactively collected data.

    Args:
        env_data: Key-value pairs for the environment (account, user, etc.).
        env_name: Name of the environment section.
        home_dir: Override for ``Path.home()`` (used in tests).

    Returns:
        The path to the written environments.yml file.
    """
    effective_home = home_dir if home_dir is not None else Path.home()
    environments_path = effective_home / ".clair" / "environments.yml"

    lines = [
        "# Clair environments -- connection settings per environment.",
        "",
        f"{env_name}:",
    ]
    for key, value in env_data.items():
        lines.append(f"  {key}: {value}")

    lines.append("")

    environments_path.parent.mkdir(parents=True, exist_ok=True)
    environments_path.write_text("\n".join(lines))
    return environments_path
