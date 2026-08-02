"""The tests of the environment loader."""

from __future__ import annotations

from pathlib import Path

import pytest

from clair.environments.environments import load_environment
from clair.exceptions import (
    EnvironmentNotFoundError,
    EnvironmentsFileNotFoundError,
    InvalidEnvironmentError,
)


class TestLoadEnvironment:
    def test_load_default_dev_environment(self, tmp_environments: Path):
        name, env = load_environment(environments_path=tmp_environments)
        assert name == "dev"
        assert env.account == "test-account"
        assert env.user == "test-user"

    def test_load_named_environment(self, tmp_environments: Path):
        name, env = load_environment(env_name="ci", environments_path=tmp_environments)
        assert name == "ci"
        assert env.user == "ci-user"

    def test_missing_environment_raises(self, tmp_environments: Path):
        with pytest.raises(EnvironmentNotFoundError, match="nonexistent"):
            load_environment(env_name="nonexistent", environments_path=tmp_environments)

    def test_missing_file_raises(self, tmp_path: Path):
        with pytest.raises(EnvironmentsFileNotFoundError):
            load_environment(environments_path=tmp_path / "nonexistent.yml")

    def test_env_var_resolution(self, tmp_environments: Path, monkeypatch):
        monkeypatch.setenv("CLAIR_ENV", "ci")
        name, _env = load_environment(environments_path=tmp_environments)
        assert name == "ci"

    def test_explicit_overrides_env_var(self, tmp_environments: Path, monkeypatch):
        monkeypatch.setenv("CLAIR_ENV", "ci")
        name, _env = load_environment(env_name="dev", environments_path=tmp_environments)
        assert name == "dev"

    def test_private_key_environment(self, tmp_environments: Path):
        name, env = load_environment(env_name="key_auth", environments_path=tmp_environments)
        assert name == "key_auth"
        assert env.private_key_path == "/secrets/snowflake_key.p8"
        assert env.private_key_passphrase is None

    def test_encrypted_private_key_environment(self, tmp_environments: Path):
        _name, env = load_environment(env_name="key_auth_encrypted", environments_path=tmp_environments)
        assert env.private_key_path == "/secrets/snowflake_key_enc.p8"
        assert env.private_key_passphrase == "s3cr3t"

    def test_environment_has_no_routing_attribute(self, tmp_environments: Path):
        _, env = load_environment(env_name="dev", environments_path=tmp_environments)
        assert not hasattr(env, "routing")


class TestUnknownKey:
    """Routing moved to the project __routing__.py. An old block must not pass silently."""

    def test_unknown_key_raises(self, tmp_environments: Path):
        with pytest.raises(InvalidEnvironmentError, match="unknown_key"):
            load_environment(env_name="unknown_key", environments_path=tmp_environments)

    def test_the_error_names_the_new_file(self, tmp_environments: Path):
        with pytest.raises(InvalidEnvironmentError, match=r"__routing__\.py"):
            load_environment(env_name="unknown_key", environments_path=tmp_environments)

    def test_a_misspelt_key_raises(self, tmp_path: Path):
        bad = tmp_path / "env.yml"
        bad.write_text("dev:\n  account: x\n  user: y\n  warehouse: z\n  wharehouse: z\n")
        with pytest.raises(InvalidEnvironmentError, match="wharehouse"):
            load_environment(environments_path=bad)

    def test_environment_without_routing_block_loads(self, tmp_path: Path):
        good = tmp_path / "env.yml"
        good.write_text("dev:\n  account: x\n  user: y\n  warehouse: z\n")
        name, env = load_environment(environments_path=good)
        assert name == "dev"
        assert env.account == "x"


class TestToConnectionDict:
    def test_externalbrowser_dict(self, tmp_environments: Path):
        _, env = load_environment(env_name="dev", environments_path=tmp_environments)
        d = env.to_connection_dict()
        assert d["account"] == "test-account"
        assert d["user"] == "test-user"
        assert d["authenticator"] == "externalbrowser"
        assert d["warehouse"] == "test_wh"
        assert "routing" not in d
        assert "password" not in d

    def test_password_dict(self, tmp_environments: Path):
        _, env = load_environment(env_name="ci", environments_path=tmp_environments)
        d = env.to_connection_dict()
        assert d["password"] == "ci-password"
        assert "authenticator" not in d

    def test_private_key_dict(self, tmp_environments: Path):
        _, env = load_environment(env_name="key_auth", environments_path=tmp_environments)
        d = env.to_connection_dict()
        assert d["private_key_path"] == "/secrets/snowflake_key.p8"
        assert "private_key_passphrase" not in d
