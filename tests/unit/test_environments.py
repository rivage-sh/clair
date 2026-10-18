"""The tests of the environment loader.

An environment holds one ``connection`` block and one ``authentication`` block.
The key in the authentication block selects the method, and each method rejects
a key of a different method. Thus two methods in one block, or a passphrase with
no key, is an error and not a silent choice.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from clair.core.scaffold import write_environments_yml
from clair.environments.environments import (
    Authenticator,
    Environment,
    Password,
    PrivateKey,
    load_environment,
)
from clair.exceptions import (
    EnvironmentNotFoundError,
    EnvironmentsFileNotFoundError,
    InvalidEnvironmentError,
)
from tests.helpers import make_environment

ENVIRONMENTS_FILE = """
dev:
  connection:
    account: test-account
    user: test-user
    warehouse: test_wh
    role: test_role
  authentication:
    authenticator: externalbrowser

ci:
  connection:
    account: test-account
    user: ci-user
    warehouse: ci_wh
  authentication:
    password: ci-password
  threads: 8

key_auth:
  connection:
    account: test-account
    user: key-user
    warehouse: key_wh
  authentication:
    private_key_path: /secrets/snowflake_key.p8
    private_key_passphrase: s3cr3t
"""


@pytest.fixture
def environments_path(tmp_path: Path) -> Path:
    path = tmp_path / "environments.yml"
    path.write_text(ENVIRONMENTS_FILE)
    return path


def write_one_environment(tmp_path: Path, block: str) -> Path:
    """Write an environments.yml that holds the dev environment only."""
    path = tmp_path / "environments.yml"
    path.write_text("dev:\n" + block)
    return path


CONNECTION_BLOCK = """\
  connection:
    account: x
    user: y
    warehouse: z
"""


class TestLoadEnvironment:
    @pytest.mark.parametrize(
        ("env_name", "expected_authentication"),
        [
            ("dev", Authenticator(authenticator="externalbrowser")),
            ("ci", Password(password="ci-password")),
            (
                "key_auth",
                PrivateKey(
                    private_key_path="/secrets/snowflake_key.p8", private_key_passphrase="s3cr3t"
                ),
            ),
        ],
    )
    def test_the_key_of_the_block_selects_the_method(
        self, environments_path: Path, env_name: str, expected_authentication
    ):
        _name, environment = load_environment(env_name=env_name, environments_path=environments_path)
        assert environment.authentication == expected_authentication

    def test_the_connection_block_holds_the_session_defaults(self, environments_path: Path):
        _name, environment = load_environment(environments_path=environments_path)
        assert environment.connection.account == "test-account"
        assert environment.connection.role == "test_role"
        assert environment.connection.region is None

    def test_threads_stays_at_the_top_level(self, environments_path: Path):
        _name, environment = load_environment(env_name="ci", environments_path=environments_path)
        assert environment.threads == 8

    def test_the_name_comes_from_the_key(self, environments_path: Path):
        name, environment = load_environment(env_name="ci", environments_path=environments_path)
        assert name == environment.name == "ci"

    def test_the_environment_variable_selects_the_name(
        self, environments_path: Path, monkeypatch: pytest.MonkeyPatch
    ):
        monkeypatch.setenv("CLAIR_ENV", "ci")
        assert load_environment(environments_path=environments_path)[0] == "ci"
        assert load_environment(env_name="dev", environments_path=environments_path)[0] == "dev"

    def test_a_missing_environment_raises(self, environments_path: Path):
        with pytest.raises(EnvironmentNotFoundError, match="nonexistent"):
            load_environment(env_name="nonexistent", environments_path=environments_path)

    def test_a_missing_file_raises(self, tmp_path: Path):
        with pytest.raises(EnvironmentsFileNotFoundError):
            load_environment(environments_path=tmp_path / "nonexistent.yml")


class TestABlockThatClairRejects:
    """Each block below loaded before, and clair then made a silent choice, or failed at the connection."""

    @pytest.mark.parametrize(
        ("block", "expected_message"),
        [
            pytest.param(
                CONNECTION_BLOCK + "  authentication:\n    password: p\n    authenticator: externalbrowser\n",
                "authenticator",
                id="two_methods",
            ),
            pytest.param(
                CONNECTION_BLOCK + "  authentication:\n    private_key_passphrase: p\n",
                "give one of password, private_key_path or authenticator",
                id="a_passphrase_with_no_key",
            ),
            pytest.param(CONNECTION_BLOCK, "authentication", id="no_authentication"),
            pytest.param(
                "  account: x\n  user: y\n  warehouse: z\n  authenticator: externalbrowser\n",
                "account",
                id="the_flat_layout",
            ),
            pytest.param(
                CONNECTION_BLOCK.replace("warehouse", "wharehouse")
                + "  authentication:\n    password: p\n",
                "wharehouse",
                id="a_misspelt_key",
            ),
            pytest.param(
                CONNECTION_BLOCK
                + "  authentication:\n    password: p\n  routing:\n    policy: database_override\n",
                "routing",
                id="a_routing_block",
            ),
        ],
    )
    def test_the_block_raises_and_names_the_key(self, tmp_path: Path, block: str, expected_message: str):
        path = write_one_environment(tmp_path, block)
        with pytest.raises(InvalidEnvironmentError, match=expected_message):
            load_environment(environments_path=path)

    def test_the_error_of_an_unknown_key_names_the_routing_file(self, tmp_path: Path):
        """An old routing block in environments.yml belongs in __routing__.py now."""
        path = write_one_environment(
            tmp_path, CONNECTION_BLOCK + "  authentication:\n    password: p\n  routing: {}\n"
        )
        with pytest.raises(InvalidEnvironmentError, match=r"__routing__\.py"):
            load_environment(environments_path=path)


@pytest.mark.parametrize(
    "authentication",
    [
        Authenticator(authenticator="externalbrowser"),
        Password(password="secret"),
        PrivateKey(private_key_path="~/.clair/key.p8"),
        PrivateKey(private_key_path="~/.clair/key.p8", private_key_passphrase="pass"),
    ],
)
def test_the_writer_and_the_loader_give_the_same_environment(
    tmp_path: Path, authentication
):
    """clair init writes the file with write_environments_yml, and each run reads it back."""
    environment = make_environment(name="dev", authentication=authentication)
    path = write_environments_yml(environment, home_dir=tmp_path)

    _name, loaded = load_environment(environments_path=path)

    assert loaded == environment
    assert isinstance(loaded, Environment)
