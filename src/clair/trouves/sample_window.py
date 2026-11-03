"""The time window of a sampled run: ``clair run --sample``.

A sampled run reads only the rows of each input in one time window, and not
the complete input. Each Trouve that gives an ``event_time`` column gets the
filter. A random sample of each input breaks the joins, because the two sides
keep different keys. A time window keeps the joins whole: an order and its
lines fall in the same window.
"""

from __future__ import annotations

import re
from datetime import UTC, datetime, timedelta

from pydantic import AwareDatetime, BaseModel, ConfigDict, model_validator

# The units of a relative window, for example "3 days".
_DURATION_UNITS = {
    "hour": timedelta(hours=1),
    "day": timedelta(days=1),
    "week": timedelta(weeks=1),
}
_DURATION_RE = re.compile(r"^\s*(\d+)\s*(hour|day|week)s?\s*$", re.IGNORECASE)
# The separator of a fixed window, as in an ISO 8601 interval.
_INTERVAL_SEPARATOR = "/"


def _as_utc(moment: datetime) -> datetime:
    """Give the moment in UTC. A moment without a time zone is UTC."""
    if moment.tzinfo is None:
        return moment.replace(tzinfo=UTC)
    return moment.astimezone(UTC)


class SampleWindow(BaseModel):
    """The rows that a sampled run reads: from *start*, up to and not including *end*.

    Attributes:
        start: The first moment of the window.
        end: The moment after the window. A row at *end* is not in the sample.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    start: AwareDatetime
    end: AwareDatetime

    @model_validator(mode="after")
    def _require_start_before_end(self) -> SampleWindow:
        if self.start >= self.end:
            raise ValueError(
                f"the sample window starts at {self.start} and ends at {self.end}. "
                "The start must be before the end."
            )
        return self

    @classmethod
    def last(cls, duration: timedelta, now: datetime | None = None) -> SampleWindow:
        """Give the window that ends at *now* and lasts *duration*.

        Args:
            duration: The length of the window.
            now: The end of the window. None takes the current time.
        """
        end = _as_utc(now) if now is not None else datetime.now(UTC)
        return cls(start=end - duration, end=end)

    @classmethod
    def parse(cls, text: str, now: datetime | None = None) -> SampleWindow:
        """Read a window from the text of the ``--sample`` option.

        Two forms are permitted:

        * A duration that ends now: ``"3 days"``, ``"6 hours"``, ``"2 weeks"``.
        * A fixed window, ``START/END``, in ISO 8601: ``"2024-07-01/2024-07-08"``
          or ``"2024-07-01T06:00/2024-07-01T18:00"``. A moment without a time
          zone is UTC.

        Raises:
            ValueError: If the text has neither form.
        """
        duration_match = _DURATION_RE.match(text)
        if duration_match:
            count = int(duration_match.group(1))
            unit = _DURATION_UNITS[duration_match.group(2).lower()]
            return cls.last(count * unit, now=now)

        if _INTERVAL_SEPARATOR in text:
            start_text, end_text = text.split(_INTERVAL_SEPARATOR, 1)
            try:
                start = datetime.fromisoformat(start_text.strip())
                end = datetime.fromisoformat(end_text.strip())
            except ValueError as exc:
                raise ValueError(
                    f"the sample window '{text}' holds a moment that is not ISO 8601: {exc}"
                ) from exc
            return cls(start=_as_utc(start), end=_as_utc(end))

        raise ValueError(
            f"clair cannot read the sample window '{text}'. Give a duration, for "
            "example '3 days', '6 hours' or '2 weeks', or a fixed window "
            "START/END, for example '2024-07-01/2024-07-08'."
        )

    def row_filter(self, event_time: str) -> str:
        """Give the WHERE condition that keeps the rows of this window.

        Clair compares the column with two TIMESTAMP_NTZ values in UTC. A
        TIMESTAMP_NTZ column that holds UTC, and a DATE column, then compare
        exactly. Snowflake converts a TIMESTAMP_TZ or a TIMESTAMP_LTZ column
        with the time zone of the session, thus the boundaries of such a column
        can move by the offset of that time zone.
        """
        start_literal = self._timestamp_literal(self.start)
        end_literal = self._timestamp_literal(self.end)
        return f"{event_time} >= {start_literal} AND {event_time} < {end_literal}"

    def reference(self, address: str, event_time: str) -> str:
        """Give the subquery that reads the rows of this window from *address*."""
        return f"(SELECT * FROM {address} WHERE {self.row_filter(event_time)})"

    @staticmethod
    def _timestamp_literal(moment: datetime) -> str:
        utc_text = moment.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S.%f")
        return f"'{utc_text}'::TIMESTAMP_NTZ"
