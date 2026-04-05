"""Tests for RunConfig validation rules."""

import pytest

from clair.trouves.run_config import IncrementalMode, UpsertConfig, RunConfig, RunMode


class TestRunConfigValidation:
    def test_default_full_refresh(self):
        config = RunConfig()
        assert config.run_mode == RunMode.FULL_REFRESH
        assert config.incremental_mode is None

    def test_full_refresh_explicit(self):
        config = RunConfig(run_mode=RunMode.FULL_REFRESH)
        assert config.run_mode == RunMode.FULL_REFRESH

    def test_incremental_append(self):
        config = RunConfig(
            run_mode=RunMode.INCREMENTAL,
            incremental_mode=IncrementalMode.APPEND,
        )
        assert config.run_mode == RunMode.INCREMENTAL
        assert config.incremental_mode == IncrementalMode.APPEND

    def test_incremental_upsert_with_primary_key_columns(self):
        config = RunConfig(
            run_mode=RunMode.INCREMENTAL,
            incremental_mode=IncrementalMode.UPSERT,
            primary_key_columns=["id"],
        )
        assert config.primary_key_columns == ["id"]

    def test_incremental_upsert_with_join_sql(self):
        config = RunConfig(
            run_mode=RunMode.INCREMENTAL,
            incremental_mode=IncrementalMode.UPSERT,
            join_sql="target.id = source.id",
        )
        assert config.join_sql == "target.id = source.id"

    def test_full_refresh_rejects_incremental_mode(self):
        with pytest.raises(ValueError, match="incremental_mode is only valid when run_mode is incremental"):
            RunConfig(
                run_mode=RunMode.FULL_REFRESH,
                incremental_mode=IncrementalMode.APPEND,
            )

    def test_incremental_requires_strategy(self):
        with pytest.raises(ValueError, match="incremental run_mode requires incremental_mode"):
            RunConfig(run_mode=RunMode.INCREMENTAL)

    def test_append_rejects_primary_key_columns(self):
        with pytest.raises(ValueError, match="primary_key_columns is only valid for upsert mode"):
            RunConfig(
                run_mode=RunMode.INCREMENTAL,
                incremental_mode=IncrementalMode.APPEND,
                primary_key_columns=["id"],
            )

    def test_append_rejects_join_sql(self):
        with pytest.raises(ValueError, match="join_sql is only valid for upsert mode"):
            RunConfig(
                run_mode=RunMode.INCREMENTAL,
                incremental_mode=IncrementalMode.APPEND,
                join_sql="target.id = source.id",
            )

    def test_upsert_rejects_both_primary_key_columns_and_join_sql(self):
        with pytest.raises(ValueError, match="specify primary_key_columns or join_sql, not both"):
            RunConfig(
                run_mode=RunMode.INCREMENTAL,
                incremental_mode=IncrementalMode.UPSERT,
                primary_key_columns=["id"],
                join_sql="target.id = source.id",
            )

    def test_upsert_requires_primary_key_columns_or_join_sql(self):
        with pytest.raises(ValueError, match="upsert mode requires primary_key_columns or join_sql"):
            RunConfig(
                run_mode=RunMode.INCREMENTAL,
                incremental_mode=IncrementalMode.UPSERT,
            )

    def test_upsert_accepts_upsert_config(self):
        config = RunConfig(
            run_mode=RunMode.INCREMENTAL,
            incremental_mode=IncrementalMode.UPSERT,
            join_sql="target.id = source.id",
            upsert_config=UpsertConfig(update_columns=["name"], insert_columns=["id", "name"]),
        )
        assert config.upsert_config is not None
        assert config.upsert_config.update_columns == ["name"]
        assert config.upsert_config.insert_columns == ["id", "name"]

    def test_append_rejects_upsert_config(self):
        with pytest.raises(ValueError, match="upsert_config is only valid for upsert mode"):
            RunConfig(
                run_mode=RunMode.INCREMENTAL,
                incremental_mode=IncrementalMode.APPEND,
                upsert_config=UpsertConfig(update_columns=["name"]),
            )
