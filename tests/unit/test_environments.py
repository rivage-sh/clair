"""Tests for environment loading."""

from __future__ import annotations

from pathlib import Path

import pytest

from clair.environments.environments import load_environment
from clair.environments.routing import SchemaIsolationRouting
from clair.exceptions import (
    EnvironmentNotFoundError,
    EnvironmentsFileNotFoundError,
    InvalidEnvironmentsFileError,
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
            load_environment(environments_path=tmp_path / "nonexistent.py")

    def test_env_var_resolution(self, tmp_environments: Path, monkeypatch):
        monkeypatch.setenv("CLAIR_ENV", "ci")
        name, env = load_environment(environments_path=tmp_environments)
        assert name == "ci"

    def test_explicit_overrides_env_var(self, tmp_environments: Path, monkeypatch):
        monkeypatch.setenv("CLAIR_ENV", "ci")
        name, env = load_environment(env_name="dev", environments_path=tmp_environments)
        assert name == "dev"

    def test_private_key_environment(self, tmp_environments: Path):
        name, env = load_environment(env_name="key_auth", environments_path=tmp_environments)
        assert name == "key_auth"
        assert env.private_key_path == "/secrets/snowflake_key.p8"
        assert env.private_key_passphrase is None

    def test_encrypted_private_key_environment(self, tmp_environments: Path):
        name, env = load_environment(env_name="key_auth_encrypted", environments_path=tmp_environments)
        assert env.private_key_path == "/secrets/snowflake_key_enc.p8"
        assert env.private_key_passphrase == "s3cr3t"

    def test_routing_parsed_for_database_override(self, tmp_environments: Path):
        _, env = load_environment(env_name="with_routing", environments_path=tmp_environments)
        assert env.routing is not None
        assert env.routing.policy == "database_override"
        assert env.routing.database_name == "OMER_DEV"

    def test_routing_parsed_for_schema_isolation(self, tmp_environments: Path):
        _, env = load_environment(env_name="with_schema_isolation", environments_path=tmp_environments)
        assert isinstance(env.routing, SchemaIsolationRouting)
        assert env.routing.policy == "schema_isolation"
        assert env.routing.database_name == "DEV"
        assert env.routing.schema_name == "obaddour"

    def test_callable_routing_loaded(self, tmp_environments: Path):
        _, env = load_environment(env_name="with_callable_routing", environments_path=tmp_environments)
        assert callable(env.routing)
        assert env.routing("refined", "products", "catalog") == "refined_dev.products.catalog"

    def test_no_routing_returns_none(self, tmp_environments: Path):
        _, env = load_environment(env_name="dev", environments_path=tmp_environments)
        assert env.routing is None

    def test_name_is_set_on_returned_environment(self, tmp_environments: Path):
        name, env = load_environment(env_name="ci", environments_path=tmp_environments)
        assert env.name == "ci"


class TestLoadEnvironmentValidation:
    def test_missing_environments_dict_raises(self, tmp_path: Path):
        bad = tmp_path / "env.py"
        bad.write_text("# no environments dict here\n")
        with pytest.raises(InvalidEnvironmentsFileError, match="'environments'"):
            load_environment(environments_path=bad)

    def test_environments_not_a_dict_raises(self, tmp_path: Path):
        bad = tmp_path / "env.py"
        bad.write_text("environments = 'not a dict'\n")
        with pytest.raises(InvalidEnvironmentsFileError, match="dict"):
            load_environment(environments_path=bad)

    def test_environment_value_not_environment_instance_raises(self, tmp_path: Path):
        bad = tmp_path / "env.py"
        bad.write_text('environments = {"dev": {"account": "x"}}\n')
        with pytest.raises(InvalidEnvironmentsFileError, match="Environment instance"):
            load_environment(environments_path=bad)

    def test_syntax_error_in_file_raises(self, tmp_path: Path):
        bad = tmp_path / "env.py"
        bad.write_text("environments = {this is not valid python}\n")
        with pytest.raises(InvalidEnvironmentsFileError):
            load_environment(environments_path=bad)


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
