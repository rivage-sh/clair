"""Run `clair init` end to end in a spare directory.

This test needs no Snowflake account. It gives the prompts a set of example
answers, then it reads the files that clair wrote. A HOME directory of the test
holds the environments.yml file, thus the test never touches the file of the
developer.
"""

from __future__ import annotations

import os
from pathlib import Path

import yaml

from tests.integration.conftest import run_clair

# The answers of the prompts of `clair init`, in the order that the CLI asks
# them. The list is the specification of the prompt order. A new prompt breaks
# this test, and that is the intent.
INIT_ANSWERS = [
    "dev",  # Environment name
    "myorg-myaccount",  # Snowflake account
    "alice@example.com",  # Snowflake user
    "1",  # Authentication method: private key
    "~/.clair/snowflake_key.p8",  # Private key path
    "n",  # Is the key encrypted?
    "example_warehouse",  # Warehouse
    "example_role",  # Role
    "us-east-1",  # Region
    "abc12345",  # Account locator
    "source.orders.raw",  # The example source table
]


def make_home(tmp_path: Path) -> dict[str, str]:
    """Give the environment of a clair subprocess with an empty HOME."""
    home_dir = tmp_path / "home"
    home_dir.mkdir()
    environment = dict(os.environ)
    environment["HOME"] = str(home_dir)
    environment["USERPROFILE"] = str(home_dir)
    environment.pop("CLAIR_ENV", None)
    return environment


def test_init_writes_a_project_that_validates(tmp_path: Path) -> None:
    """`clair init` writes a project, and `clair validate` accepts it."""
    environment = make_home(tmp_path)
    project_dir = tmp_path / "spare_project"

    completed = run_clair(
        ["init", "--project", str(project_dir)],
        environment,
        stdin_text="\n".join(INIT_ANSWERS) + "\n",
    )
    assert "Project ready" in completed.stdout

    # The project files.
    assert (project_dir / "source" / "orders" / "raw.py").is_file()
    assert (project_dir / "__routing__.py").is_file()
    assert (project_dir / ".gitignore").read_text().strip() == "/_clairtifacts"

    # The connection profile, in the HOME of the test.
    environments_file = Path(environment["HOME"]) / ".clair" / "environments.yml"
    environments = yaml.safe_load(environments_file.read_text())
    assert environments["dev"]["account"] == "myorg-myaccount"
    assert environments["dev"]["user"] == "alice@example.com"
    assert environments["dev"]["warehouse"] == "example_warehouse"
    assert environments["dev"]["role"] == "example_role"
    assert environments["dev"]["private_key_path"] == "~/.clair/snowflake_key.p8"

    # The routing file of the scaffold names the dev environment, thus validate
    # writes no warning about a passthrough.
    validated = run_clair(
        ["validate", "--project", str(project_dir), "--env", "dev"], environment
    )
    assert "does not name the environment" not in validated.stdout


def test_init_keeps_an_environments_file_that_exists(tmp_path: Path) -> None:
    """`clair init` does not replace the connection profile of the user."""
    environment = make_home(tmp_path)
    clair_dir = Path(environment["HOME"]) / ".clair"
    clair_dir.mkdir()
    original_text = "dev:\n  account: keep-me\n  user: keep-me\n  warehouse: keep_wh\n"
    (clair_dir / "environments.yml").write_text(original_text)

    project_dir = tmp_path / "second_project"
    run_clair(
        ["init", "--project", str(project_dir)],
        environment,
        # The CLI asks for the source table only, because the profile exists.
        stdin_text="source.orders.raw\n",
    )

    assert (clair_dir / "environments.yml").read_text() == original_text
