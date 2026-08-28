"""The tests of `clair clean`, through the Python API.

``parse_before_spec`` takes the current time as an argument, thus each test
gives a fixed time and patches no clock. ``clair.clean()`` gives a CleanOutput,
thus a test names the runs that clair removed and it reads no line of output.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

import clair
from clair.core.artifacts import (
    InvalidBeforeSpecError,
    find_artifact_runs,
    parse_before_spec,
    run_id_to_time,
    select_runs_to_remove,
)

# A Thursday. The tests of the calendar words count from this day.
NOW = datetime(2026, 3, 19, 15, 0, 0, tzinfo=UTC)


def make_run_id(created_at: datetime) -> str:
    """Make a UUIDv7 hex run_id that holds the given UTC time."""
    milliseconds = int(created_at.timestamp() * 1000)
    # The time occupies 12 hex characters. Add more characters, to a total of 32.
    return f"{milliseconds:012x}" + "0" * 20


def make_artifacts(project_dir: Path, times: dict[str, datetime]) -> dict[str, Path]:
    """Make one artifact run directory for each name, and give the paths.

    Args:
        project_dir: The root of the project.
        times: The creation time of each run, by a name that the test uses.

    Returns:
        The path of each run directory, by the same name.
    """
    artifacts_dir = project_dir / "_clairtifacts"
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    paths = {}
    for name, created_at in times.items():
        run_dir = artifacts_dir / make_run_id(created_at)
        run_dir.mkdir()
        (run_dir / "compiled.sql").write_text("select 1")
        paths[name] = run_dir
    return paths


class TestRunIdToTime:
    """A run_id holds the time of the run in its first 12 hex characters."""

    def test_a_run_id_gives_its_time(self):
        created_at = datetime(2026, 3, 19, 12, 0, 0, tzinfo=UTC)
        result = run_id_to_time(make_run_id(created_at))
        assert result is not None
        # The accuracy after the two steps is one millisecond.
        assert abs((result - created_at).total_seconds()) < 0.001

    def test_the_time_is_utc(self):
        result = run_id_to_time(make_run_id(datetime(2026, 1, 1, tzinfo=UTC)))
        assert result is not None
        assert result.tzinfo == UTC

    @pytest.mark.parametrize(
        "run_id",
        [
            pytest.param("abc123", id="too_short"),
            pytest.param("z" * 32, id="not_hex"),
            pytest.param("", id="empty"),
            pytest.param("0" * 33, id="too_long"),
        ],
    )
    def test_a_name_that_is_not_a_run_id_gives_none(self, run_id: str):
        assert run_id_to_time(run_id) is None


class TestParseBeforeSpec:
    """The --before value gives a UTC limit."""

    @pytest.mark.parametrize(
        ("spec", "expected_delta"),
        [
            ("7d", timedelta(days=7)),
            ("1d", timedelta(days=1)),
            ("24h", timedelta(hours=24)),
            ("30m", timedelta(minutes=30)),
            ("0d", timedelta(0)),
        ],
    )
    def test_a_duration_counts_back_from_now(
        self, spec: str, expected_delta: timedelta
    ):
        assert parse_before_spec(spec, NOW) == NOW - expected_delta

    @pytest.mark.parametrize(
        ("spec", "expected"),
        [
            ("2026-03-01", datetime(2026, 3, 1, tzinfo=UTC)),
            (
                "2026-03-01T12:00:00+00:00",
                datetime(2026, 3, 1, 12, 0, 0, tzinfo=UTC),
            ),
            ("2026-03-01T08:00:00", datetime(2026, 3, 1, 8, 0, 0, tzinfo=UTC)),
        ],
    )
    def test_an_iso_time_gives_that_time(self, spec: str, expected: datetime):
        assert parse_before_spec(spec, NOW) == expected

    def test_an_iso_time_with_no_zone_is_utc(self):
        assert parse_before_spec("2026-03-01T08:00:00", NOW).tzinfo == UTC

    @pytest.mark.parametrize(
        "spec",
        [
            pytest.param("not-a-date", id="words"),
            pytest.param("5y", id="unknown_unit"),
            pytest.param("", id="empty"),
            pytest.param("7 d", id="a_space"),
            pytest.param("-7d", id="a_minus"),
        ],
    )
    def test_a_value_that_clair_cannot_read_raises(self, spec: str):
        with pytest.raises(InvalidBeforeSpecError):
            parse_before_spec(spec, NOW)


class TestCalendarWords:
    """A calendar word starts at local midnight, and clair gives it in UTC."""

    def test_today_is_the_local_midnight_of_now(self):
        expected = NOW.astimezone().replace(
            hour=0, minute=0, second=0, microsecond=0
        ).astimezone(UTC)
        assert parse_before_spec("today", NOW) == expected

    def test_yesterday_is_one_day_before_today(self):
        assert parse_before_spec("yesterday", NOW) == parse_before_spec(
            "today", NOW
        ) - timedelta(days=1)

    def test_last_week_is_the_monday_of_the_week_before(self):
        """2026-03-19 is a Thursday, thus this Monday is the 16th."""
        result = parse_before_spec("last_week", NOW)
        assert result <= parse_before_spec("today", NOW) - timedelta(days=7)
        assert result.astimezone().weekday() == 0

    def test_each_word_is_before_now(self):
        for spec in ("today", "yesterday", "last_week"):
            assert parse_before_spec(spec, NOW) < NOW

    def test_the_words_are_in_order(self):
        assert (
            parse_before_spec("last_week", NOW)
            < parse_before_spec("yesterday", NOW)
            < parse_before_spec("today", NOW)
        )


class TestSelectRunsToRemove:
    """The cutoff keeps the runs that clair made after it."""

    def test_no_cutoff_selects_each_run(self, tmp_path: Path):
        make_artifacts(
            tmp_path,
            {"old": NOW - timedelta(days=30), "new": NOW - timedelta(minutes=1)},
        )
        runs = find_artifact_runs(tmp_path / "_clairtifacts")
        assert len(select_runs_to_remove(runs, None)) == 2

    def test_a_cutoff_keeps_the_runs_after_it(self, tmp_path: Path):
        make_artifacts(
            tmp_path,
            {"old": NOW - timedelta(days=30), "new": NOW - timedelta(minutes=1)},
        )
        runs = find_artifact_runs(tmp_path / "_clairtifacts")
        selected = select_runs_to_remove(runs, NOW - timedelta(days=7))
        assert [run.created_at for run in selected] == [
            run.created_at for run in runs if run.created_at == NOW - timedelta(days=30)
        ]

    def test_a_run_exactly_at_the_cutoff_stays(self, tmp_path: Path):
        """The rule is `before` the cutoff, thus the cutoff itself is not before."""
        make_artifacts(tmp_path, {"edge": NOW})
        runs = find_artifact_runs(tmp_path / "_clairtifacts")
        assert select_runs_to_remove(runs, NOW) == []

    def test_a_directory_that_is_not_a_run_stays(self, tmp_path: Path):
        """Clair did not make it, thus a cutoff must not remove it."""
        artifacts_dir = tmp_path / "_clairtifacts"
        artifacts_dir.mkdir()
        (artifacts_dir / "notes").mkdir()
        runs = find_artifact_runs(artifacts_dir)
        assert select_runs_to_remove(runs, NOW) == []

    def test_no_cutoff_removes_a_directory_that_is_not_a_run(self, tmp_path: Path):
        artifacts_dir = tmp_path / "_clairtifacts"
        artifacts_dir.mkdir()
        (artifacts_dir / "notes").mkdir()
        runs = find_artifact_runs(artifacts_dir)
        assert [run.run_id for run in select_runs_to_remove(runs, None)] == ["notes"]

    def test_an_absent_directory_gives_no_run(self, tmp_path: Path):
        assert find_artifact_runs(tmp_path / "_clairtifacts") == []

    def test_a_file_is_not_a_run(self, tmp_path: Path):
        artifacts_dir = tmp_path / "_clairtifacts"
        artifacts_dir.mkdir()
        (artifacts_dir / "a_file.txt").write_text("x")
        assert find_artifact_runs(artifacts_dir) == []


class TestClean:
    """`clair.clean()` removes the run directories."""

    def test_it_removes_each_run(self, tmp_path: Path):
        paths = make_artifacts(tmp_path, {"one": NOW, "two": NOW - timedelta(days=1)})
        output = clair.clean(tmp_path)
        assert output.run_count == 2
        assert not paths["one"].exists()
        assert not paths["two"].exists()

    def test_a_dry_run_removes_nothing(self, tmp_path: Path):
        paths = make_artifacts(tmp_path, {"one": NOW})
        output = clair.clean(tmp_path, dry_run=True)
        assert output.run_count == 1
        assert output.dry_run is True
        assert paths["one"].exists()

    def test_before_keeps_the_new_runs(self, tmp_path: Path):
        paths = make_artifacts(
            tmp_path,
            {"old": NOW - timedelta(days=30), "new": NOW - timedelta(hours=1)},
        )
        output = clair.clean(tmp_path, before="7d", now=NOW)
        assert output.run_count == 1
        assert not paths["old"].exists()
        assert paths["new"].exists()

    def test_the_output_holds_the_cutoff(self, tmp_path: Path):
        make_artifacts(tmp_path, {"one": NOW})
        output = clair.clean(tmp_path, before="7d", now=NOW, dry_run=True)
        assert output.cutoff == NOW - timedelta(days=7)

    def test_no_before_gives_no_cutoff(self, tmp_path: Path):
        make_artifacts(tmp_path, {"one": NOW})
        assert clair.clean(tmp_path, dry_run=True).cutoff is None

    def test_an_absent_artifacts_directory_removes_nothing(self, tmp_path: Path):
        output = clair.clean(tmp_path)
        assert output.artifacts_dir_exists is False
        assert output.run_count == 0

    def test_an_empty_artifacts_directory_removes_nothing(self, tmp_path: Path):
        (tmp_path / "_clairtifacts").mkdir()
        output = clair.clean(tmp_path)
        assert output.artifacts_dir_exists is True
        assert output.run_count == 0

    def test_it_names_the_run_that_it_removed(self, tmp_path: Path):
        make_artifacts(tmp_path, {"one": NOW})
        output = clair.clean(tmp_path)
        assert output.run_ids == [make_run_id(NOW)]

    def test_a_bad_before_value_raises(self, tmp_path: Path):
        make_artifacts(tmp_path, {"one": NOW})
        with pytest.raises(InvalidBeforeSpecError):
            clair.clean(tmp_path, before="not-a-date")

    def test_a_bad_before_value_removes_nothing(self, tmp_path: Path):
        paths = make_artifacts(tmp_path, {"one": NOW})
        with pytest.raises(InvalidBeforeSpecError):
            clair.clean(tmp_path, before="not-a-date")
        assert paths["one"].exists()
