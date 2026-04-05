"""Tests for environment loading."""

from __future__ import annotations

from pathlib import Path

import pytest

from clair.environments.environments import load_environment
from clair.environments.routing import SchemaIsolationRouting
from clair.exceptions import (
    EnvironmentNotFoundError,
    EnvironmentsFileNotFoundError,
    InvalidRoutingConfigError,
    InvalidRoutingPolicyError,
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

    def test_no_routing_returns_none(self, tmp_environments: Path):
        _, env = load_environment(env_name="dev", environments_path=tmp_environments)
        assert env.routing is None


class TestLoadEnvironmentValidation:
    def test_missing_policy_raises(self, tmp_path: Path):
        bad = tmp_path / "env.yml"
        bad.write_text("dev:\n  account: x\n  user: y\n  warehouse: z\n  routing:\n    database_name: FOO\n")
        with pytest.raises(InvalidRoutingConfigError, match="policy"):
            load_environment(environments_path=bad)

    def test_unknown_policy_raises(self, tmp_path: Path):
        bad = tmp_path / "env.yml"
        bad.write_text("dev:\n  account: x\n  user: y\n  warehouse: z\n  routing:\n    policy: nonsense\n    database_name: FOO\n")
        with pytest.raises(InvalidRoutingPolicyError, match="nonsense"):
            load_environment(environments_path=bad)

    def test_schema_isolation_missing_schema_name_raises(self, tmp_path: Path):
        bad = tmp_path / "env.yml"
        bad.write_text("dev:\n  account: x\n  user: y\n  warehouse: z\n  routing:\n    policy: schema_isolation\n    database_name: DEV\n")
        with pytest.raises(InvalidRoutingConfigError, match="schema_name"):
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
