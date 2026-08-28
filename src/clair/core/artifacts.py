"""The artifacts directory: read the run times, and choose the runs to remove.

`clair clean` deletes the compiled artifacts of the old runs. Each function here
is pure: ``parse_before_spec`` takes the current time as an argument, and it
reads no clock. Thus a test gives a fixed time, and it patches nothing.
"""

from __future__ import annotations

import re
from datetime import UTC, datetime, timedelta
from pathlib import Path

from pydantic import BaseModel, computed_field

from clair.exceptions import ClairError

# The number of hex characters of a UUIDv7 that hold the milliseconds.
_RUN_ID_TIME_LENGTH = 12

# The complete length of a run_id in hex characters.
_RUN_ID_LENGTH = 32

# A --before value that names a time span, for example "7d", "24h" or "30m".
_DURATION_PATTERN = re.compile(r"^(\d+)([dhm])$")

BEFORE_SPEC_HELP = (
    "Use 'today', 'yesterday', 'last_week', a duration such as '7d' or '24h', "
    "or an ISO date such as '2026-03-01'."
)


class InvalidBeforeSpecError(ClairError):
    """The --before value of `clair clean` is not a time that clair reads."""


class ArtifactRun(BaseModel):
    """One run directory below `_clairtifacts/`.

    Attributes:
        path: The directory of the run.
        run_id: The name of the directory, which is the run_id.
        created_at: The time in the run_id, or None if the name is not a run_id.
    """

    path: Path
    run_id: str
    created_at: datetime | None


class CleanOutput(BaseModel):
    """The result after clair chose the runs to remove.

    Attributes:
        artifacts_dir: The `_clairtifacts/` directory of the project.
        cutoff: The limit that --before gave, or None for each run.
        runs: The runs that clair removed, or that a dry run names.
        dry_run: True if clair removed nothing.
    """

    artifacts_dir: Path
    cutoff: datetime | None
    runs: list[ArtifactRun]
    dry_run: bool

    @computed_field  # type: ignore[prop-decorator]
    @property
    def artifacts_dir_exists(self) -> bool:
        """False if the project holds no artifacts directory."""
        return self.artifacts_dir.exists()

    @property
    def run_count(self) -> int:
        """Give the number of runs that clair removed or names."""
        return len(self.runs)

    @property
    def run_ids(self) -> list[str]:
        """Give the run_id of each run, in the order that clair removes them."""
        return [run.run_id for run in self.runs]


def run_id_to_time(run_id: str) -> datetime | None:
    """Read the UTC creation time from a UUIDv7 hex run_id.

    Returns:
        The time, or None if *run_id* is not a UUIDv7 hex string.
    """
    if len(run_id) != _RUN_ID_LENGTH:
        return None
    try:
        milliseconds = int(run_id[:_RUN_ID_TIME_LENGTH], 16)
    except ValueError:
        return None
    return datetime.fromtimestamp(milliseconds / 1000, tz=UTC)


def parse_before_spec(spec: str, now: datetime) -> datetime:
    """Read a --before value and give the equivalent UTC limit.

    The function accepts these forms:
        - Usual words: 'today', 'yesterday', 'last_week'
        - A time span: '7d', '24h', '30m'
        - An ISO date or time: '2026-03-01', '2026-03-01T12:00:00'

    Args:
        spec: The value of --before.
        now: The current time. The caller gives it, thus this function reads no
            clock and a test needs no patch.

    Returns:
        The UTC time. Clair removes each run before this time.

    Raises:
        InvalidBeforeSpecError: If clair cannot read *spec*.
    """
    # A calendar limit starts at local midnight. The code then changes the time
    # to UTC. Thus "today" is today in the time zone of the user, not in UTC.
    local_today = now.astimezone().replace(
        hour=0, minute=0, second=0, microsecond=0
    ).astimezone(UTC)

    if spec == "today":
        return local_today
    if spec == "yesterday":
        return local_today - timedelta(days=1)
    if spec == "last_week":
        # The Monday of the week before, in local time, changed to UTC.
        this_monday = local_today - timedelta(days=local_today.astimezone().weekday())
        return this_monday - timedelta(weeks=1)

    duration = _DURATION_PATTERN.match(spec)
    if duration:
        count = int(duration.group(1))
        unit = duration.group(2)
        if unit == "d":
            return now - timedelta(days=count)
        elif unit == "h":
            return now - timedelta(hours=count)
        elif unit == "m":
            return now - timedelta(minutes=count)
        else:
            raise InvalidBeforeSpecError(f"Clair does not know the time unit {unit}.")

    try:
        given_time = datetime.fromisoformat(spec)
    except ValueError:
        raise InvalidBeforeSpecError(
            f"Clair cannot read '{spec}'. {BEFORE_SPEC_HELP}"
        ) from None

    if given_time.tzinfo is None:
        return given_time.replace(tzinfo=UTC)
    return given_time


def find_artifact_runs(artifacts_dir: Path) -> list[ArtifactRun]:
    """Give each run directory below *artifacts_dir*, in name order."""
    if not artifacts_dir.exists():
        return []
    return [
        ArtifactRun(
            path=entry,
            run_id=entry.name,
            created_at=run_id_to_time(entry.name),
        )
        for entry in sorted(artifacts_dir.iterdir())
        if entry.is_dir()
    ]


def select_runs_to_remove(
    runs: list[ArtifactRun], cutoff: datetime | None
) -> list[ArtifactRun]:
    """Give the runs that clair removes.

    A cutoff of None selects each run. With a cutoff, clair keeps a run that it
    cannot date, because the name is then not a run_id and clair did not make
    the directory.
    """
    if cutoff is None:
        return list(runs)
    return [
        run
        for run in runs
        if run.created_at is not None and run.created_at < cutoff
    ]
