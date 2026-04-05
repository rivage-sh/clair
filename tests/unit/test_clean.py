"""Tests for clair clean helpers: _parse_before_spec and _run_id_to_time."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest

from clair.cli.main import _parse_before_spec, _run_id_to_time

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

UTC = timezone.utc


def _make_run_id(dt: datetime) -> str:
    """Build a fake UUIDv7 hex run_id encoding the given UTC datetime."""
    ts_ms = int(dt.timestamp() * 1000)
    # 12 hex chars for the timestamp, padded to 32 chars total
    return f"{ts_ms:012x}" + "0" * 20


# ---------------------------------------------------------------------------
# _run_id_to_time
# ---------------------------------------------------------------------------


class TestRunIdToTime:
    def test_decodes_known_timestamp(self):
        dt = datetime(2026, 3, 19, 12, 0, 0, tzinfo=UTC)
        run_id = _make_run_id(dt)
        result = _run_id_to_time(run_id)
        assert result is not None
        # Round-trip precision is milliseconds
        assert abs((result - dt).total_seconds()) < 0.001

    def test_returns_utc(self):
        dt = datetime(2026, 1, 1, tzinfo=UTC)
        result = _run_id_to_time(_make_run_id(dt))
        assert result is not None
        assert result.tzinfo == UTC

    def test_returns_none_for_short_string(self):
        assert _run_id_to_time("abc123") is None

    def test_returns_none_for_non_hex(self):
        assert _run_id_to_time("z" * 32) is None

    def test_returns_none_for_empty_string(self):
        assert _run_id_to_time("") is None


# ---------------------------------------------------------------------------
# _parse_before_spec — duration lookbacks
# ---------------------------------------------------------------------------


class TestParseDurations:
    def test_7d(self):
        now = datetime(2026, 3, 19, 15, 0, 0, tzinfo=UTC)
        with patch("clair.cli.main.datetime") as mock_dt:
            mock_dt.now.return_value = now
            mock_dt.fromisoformat.side_effect = datetime.fromisoformat
            result = _parse_before_spec("7d")
        assert result == now - timedelta(days=7)

    def test_24h(self):
        now = datetime(2026, 3, 19, 15, 0, 0, tzinfo=UTC)
        with patch("clair.cli.main.datetime") as mock_dt:
            mock_dt.now.return_value = now
            mock_dt.fromisoformat.side_effect = datetime.fromisoformat
            result = _parse_before_spec("24h")
        assert result == now - timedelta(hours=24)

    def test_30m(self):
        now = datetime(2026, 3, 19, 15, 0, 0, tzinfo=UTC)
        with patch("clair.cli.main.datetime") as mock_dt:
            mock_dt.now.return_value = now
            mock_dt.fromisoformat.side_effect = datetime.fromisoformat
            result = _parse_before_spec("30m")
        assert result == now - timedelta(minutes=30)


# ---------------------------------------------------------------------------
# _parse_before_spec — ISO dates
# ---------------------------------------------------------------------------


class TestParseIsoDates:
    def test_date_only(self):
        result = _parse_before_spec("2026-03-01")
        assert result == datetime(2026, 3, 1, tzinfo=UTC)

    def test_datetime_with_tz(self):
        result = _parse_before_spec("2026-03-01T12:00:00+00:00")
        assert result == datetime(2026, 3, 1, 12, 0, 0, tzinfo=UTC)

    def test_naive_datetime_assumed_utc(self):
        result = _parse_before_spec("2026-03-01T08:00:00")
        assert result == datetime(2026, 3, 1, 8, 0, 0, tzinfo=UTC)


# ---------------------------------------------------------------------------
# _parse_before_spec — natural language
# ---------------------------------------------------------------------------


class TestParseNaturalLanguage:
    # We mock datetime.now() (no-arg, for local time) to a fixed local time.
    # Use a naive datetime to simulate what datetime.now() returns (no tzinfo).
    _LOCAL_NOW = datetime(2026, 3, 19, 22, 30, 0)  # Wednesday 10:30pm local

    def _patch(self):
        """Return a context manager that freezes local and UTC time."""
        local_now = self._LOCAL_NOW
        utc_now = datetime(2026, 3, 20, 3, 30, 0, tzinfo=UTC)  # UTC+5h ahead

        class _MockDatetime:
            @staticmethod
            def now(tz=None):
                if tz is not None:
                    return utc_now
                return local_now

            @staticmethod
            def fromisoformat(s):
                return datetime.fromisoformat(s)

        return patch("clair.cli.main.datetime", _MockDatetime)

    def test_today_is_local_midnight(self):
        with self._patch():
            result = _parse_before_spec("today")
        # Local midnight 2026-03-19 00:00 converted to UTC
        local_midnight = datetime(2026, 3, 19, 0, 0, 0)
        expected = local_midnight.astimezone(UTC)
        assert result == expected

    def test_yesterday_is_one_day_before_local_midnight(self):
        with self._patch():
            result = _parse_before_spec("yesterday")
        local_midnight = datetime(2026, 3, 19, 0, 0, 0)
        expected = (local_midnight - timedelta(days=1)).astimezone(UTC)
        assert result == expected

    def test_last_week_is_monday_of_prior_week(self):
        # 2026-03-19 is a Thursday (weekday=3).
        # This Monday = 2026-03-16; last Monday = 2026-03-09.
        with self._patch():
            result = _parse_before_spec("last_week")
        last_monday_local = datetime(2026, 3, 9, 0, 0, 0)
        expected = last_monday_local.astimezone(UTC)
        assert result == expected

    def test_today_differs_from_utc_midnight_when_offset(self):
        # Sanity check: local midnight != UTC midnight when there's an offset.
        with self._patch():
            today_result = _parse_before_spec("today")
        utc_midnight = datetime(2026, 3, 20, 0, 0, 0, tzinfo=UTC)  # UTC "today"
        # They should differ (local is behind UTC in this scenario)
        assert today_result != utc_midnight


# ---------------------------------------------------------------------------
# _parse_before_spec — invalid input
# ---------------------------------------------------------------------------


class TestParseInvalidInput:
    def test_garbage_string_raises(self):
        with pytest.raises(Exception):
            _parse_before_spec("not-a-date")

    def test_wrong_unit_raises(self):
        with pytest.raises(Exception):
            _parse_before_spec("5y")
