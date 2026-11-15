"""The artifacts directory: write the result of an operation, and remove the old runs.

An operation in ``clair.api`` gives a result object, and it writes no file. The
writers here put that result on disk, below an artifacts directory that the
caller names. The CLI names ``<project_root>/_clairtifacts``. A notebook that
calls no writer writes nothing.

Each run has one directory, ``<artifacts_dir>/<run_id>/``. Below it, the file of
a Trouve follows the physical address: one directory for the database name, one
for the schema name, and the table name as the file name.

`clair clean` deletes the artifacts of the old runs. ``parse_before_spec`` takes
the current time as an argument, and it reads no clock. Thus a test gives a
fixed time, and it patches nothing.
"""

from __future__ import annotations

import re
from datetime import UTC, datetime, timedelta
from pathlib import Path

from pydantic import BaseModel, computed_field

from clair.adapters.objects import Statement
from clair.core.compiler import CompiledNodeInfo, CompileOutput
from clair.core.runner import RunCancel, RunFailure, RunResult, RunSkip, RunSummary
from clair.exceptions import ClairError
from clair.phase import Phase
from clair.trouves.address import NodeAddresses
from clair.trouves.plan import TestResult

# The number of hex characters of a UUIDv7 that hold the milliseconds.
_RUN_ID_TIME_LENGTH = 12

# The complete length of a run_id in hex characters.
_RUN_ID_LENGTH = 32

# A --before value that names a time span, for example "7d", "24h" or "30m".
_DURATION_PATTERN = re.compile(r"^(\d+)([dhm])$")

# The file below the run directory that holds the complete RunSummary as JSON.
RUN_SUMMARY_FILE_NAME = "run_summary.json"

# The text between two statements of one artifact file.
_STATEMENT_SEPARATOR = "\n\n---\n\n"

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


def run_directory(artifacts_dir: Path, run_id: str) -> Path:
    """Give the directory of one run below *artifacts_dir*."""
    return artifacts_dir / run_id


def _node_file(run_dir: Path, addresses: NodeAddresses, suffix: str) -> Path:
    """Give the artifact file of one Trouve. Its path follows the physical address."""
    physical_address = addresses.physical
    return (
        run_dir
        / physical_address.database_name
        / physical_address.schema_name
        / f"{physical_address.table_name}{suffix}"
    )


def _write_text(path: Path, text: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)
    return path


def _render_statements_file(node: CompiledNodeInfo) -> str:
    """Give the text of the ``.sql`` artifact file of one compiled Trouve."""
    return _STATEMENT_SEPARATOR.join(statement.strip() for statement in node.sql) + "\n"


def write_compile_output(artifacts_dir: Path, output: CompileOutput) -> Path:
    """Write the files of each compiled Trouve, and give the run directory.

    Each Trouve gives a ``.sql`` file that holds its statements. A pandas
    Trouve also gives a ``.py`` file that holds its Python text.
    """
    run_dir = run_directory(artifacts_dir, output.run_id)
    run_dir.mkdir(parents=True, exist_ok=True)
    for node in output.compiled_nodes:
        _write_text(_node_file(run_dir, node.addresses, ".sql"), _render_statements_file(node))
        if node.python_source is not None:
            _write_text(_node_file(run_dir, node.addresses, ".py"), node.python_source)
    return run_dir


def _comment_lines(label: str, text: str) -> list[str]:
    """Give *text* as SQL comment lines. Each line of a long message stays a comment."""
    lines = text.strip().splitlines() or [""]
    return [f"-- {label}: {lines[0]}", *(f"--   {line}" for line in lines[1:])]


def _render_statement(statement: Statement, index: int, total: int, label: str = "") -> str:
    """Give one executed statement, with a comment that holds the answer of the warehouse."""
    header = f"-- statement {index}/{total}: {statement.status.value}"
    if statement.query_id:
        header += f", query ID {statement.query_id}"
    lines = [header]
    if label:
        lines.append(f"-- {label}")
    if statement.query_url:
        lines.append(f"-- URL: {statement.query_url}")
    if statement.error:
        lines.extend(_comment_lines("error", statement.error))
    lines.append(statement.sql.strip())
    return "\n".join(lines)


def _test_label(test_result: TestResult) -> str:
    """Give the comment that names one test and its outcome."""
    column = f" ({test_result.column_name})" if test_result.column_name else ""
    if test_result.passed:
        outcome = "passed"
    elif test_result.error:
        outcome = "the query failed"
    else:
        row_word = "row" if test_result.failing_row_count == 1 else "rows"
        outcome = f"{test_result.failing_row_count} failing {row_word}"
    return f"{test_result.test_type}{column}, {test_result.severity.value.upper()}: {outcome}"


def _render_run_result(result: RunResult) -> str:
    """Give the text of the artifact file of one Trouve that clair ran.

    The statements come in sections, one for each phase, in the order of
    ``Phase``, with the headers of ``clair compile``. Thus the record of the
    run reads the same way as the plan. The ``test`` section holds each test
    query, and its outcome.
    """
    assert not isinstance(result, RunSkip | RunCancel)
    lines = [
        f"-- clair run: {result.addresses.physical}",
        f"-- status: {result.status.value}",
    ]
    if isinstance(result, RunFailure):
        if result.failed_phase is not None:
            lines.append(f"-- failed phase: {result.failed_phase.value}")
        lines.extend(_comment_lines("error", result.error))

    # Each statement with its phase and its label, in the order of the file.
    entries: list[tuple[Phase | None, Statement, str]] = [
        (None, statement, "") for statement in result.statements if statement.phase is None
    ]
    for phase in Phase:
        if phase == Phase.TEST:
            entries.extend(
                (phase, test_result.statement, _test_label(test_result))
                for test_result in result.test_results
            )
            continue
        entries.extend(
            (phase, statement, "") for statement in result.statements if statement.phase == phase
        )

    total = len(entries)
    parts = ["\n".join(lines)]
    previous_phase: Phase | None = None
    for index, (phase, statement, label) in enumerate(entries, 1):
        text = _render_statement(statement, index, total, label)
        if phase is not None and phase != previous_phase:
            text = f"{phase.header}\n{text}"
        previous_phase = phase
        parts.append(text)
    return _STATEMENT_SEPARATOR.join(parts) + "\n"


def write_run_result(artifacts_dir: Path, run_id: str, result: RunResult) -> Path | None:
    """Write the statements of one Trouve that clair ran, and give the file.

    A ``RunHandle`` gives each result when its Trouve completes, thus the
    caller calls this function in its loop over the handle. The directory then
    fills while the run continues, and a run that stops keeps the files of each
    Trouve that completed.

    The ``.sql`` file holds each statement, with its status, its query ID and
    its error. A skipped Trouve sent no statement, thus it gives no file and
    this function gives None. The run summary names it.
    """
    if isinstance(result, RunSkip | RunCancel):
        return None
    run_dir = run_directory(artifacts_dir, run_id)
    return _write_text(_node_file(run_dir, result.addresses, ".sql"), _render_run_result(result))


def write_run_summary(artifacts_dir: Path, summary: RunSummary) -> Path:
    """Write the complete RunSummary as JSON, and give the file.

    ``RunSummary.model_validate_json`` reads the file back.
    """
    run_dir = run_directory(artifacts_dir, summary.run_id)
    return _write_text(run_dir / RUN_SUMMARY_FILE_NAME, summary.model_dump_json(indent=2) + "\n")
