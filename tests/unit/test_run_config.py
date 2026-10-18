"""The tests of the run configuration types.

``RunConfig`` is ``FullRefresh | Incremental``, and each strategy holds only the
fields that it reads. Thus most illegal states have no constructor, and the
tests below cover the few rules that a type cannot state.
"""

import pytest
from pydantic import TypeAdapter, ValidationError

from clair.trouves.run_config import (
    Append,
    DeleteInsert,
    FullRefresh,
    Incremental,
    RunConfig,
    RunMode,
    Upsert,
)


def test_each_config_names_its_run_mode():
    assert FullRefresh().run_mode == RunMode.FULL_REFRESH
    assert Incremental().run_mode == RunMode.INCREMENTAL


def test_an_incremental_config_without_a_strategy_is_valid():
    """A build_sql() override writes its own incremental SQL."""
    assert Incremental().strategy is None


@pytest.mark.parametrize("strategy_type", [Upsert, DeleteInsert])
class TestEachStrategyThatMatchesRows:
    """Upsert and DeleteInsert both match a new row to a target row."""

    @pytest.mark.parametrize(
        ("match_arguments", "expected_condition"),
        [
            ({"primary_key_columns": ["id"]}, "target.id = source.id"),
            (
                {"primary_key_columns": ["id", "region"]},
                "target.id = source.id AND target.region = source.region",
            ),
            ({"join_sql": "target.day = source.day"}, "target.day = source.day"),
        ],
    )
    def test_a_key_or_a_join_makes_the_match_condition(
        self, strategy_type, match_arguments, expected_condition
    ):
        assert strategy_type(**match_arguments).match_condition == expected_condition

    def test_no_key_and_no_join_raises(self, strategy_type):
        with pytest.raises(
            ValidationError, match=f"{strategy_type.__name__} needs primary_key_columns or join_sql"
        ):
            strategy_type()

    def test_a_key_and_a_join_together_raise(self, strategy_type):
        with pytest.raises(ValidationError, match="but not both"):
            strategy_type(primary_key_columns=["id"], join_sql="target.id = source.id")


@pytest.mark.parametrize(
    ("config_type", "arguments"),
    [
        # Each one is a field of the flat RunConfig that clair replaced. An
        # unknown field must fail, and not disappear in silence.
        (Incremental, {"incremental_mode": "append"}),
        (Incremental, {"primary_key_columns": ["id"]}),
        (FullRefresh, {"strategy": Append()}),
        (Append, {"primary_key_columns": ["id"]}),
        (DeleteInsert, {"primary_key_columns": ["id"], "update_columns": ["name"]}),
    ],
)
def test_a_field_that_the_config_does_not_read_raises(config_type, arguments):
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        config_type(**arguments)


@pytest.mark.parametrize(
    "run_config",
    [
        FullRefresh(),
        Incremental(),
        Incremental(strategy=Append()),
        Incremental(strategy=Upsert(primary_key_columns=["id"], update_columns=["name"])),
        Incremental(strategy=DeleteInsert(join_sql="target.day = source.day")),
    ],
)
def test_a_config_survives_a_round_trip_through_json(run_config):
    """The catalog writes the config as JSON, and the discriminators read it back."""
    adapter = TypeAdapter(RunConfig)
    assert adapter.validate_json(adapter.dump_json(run_config)) == run_config
