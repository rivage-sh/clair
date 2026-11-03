"""Prove a sampled run against Snowflake: ``clair.run(sample=...)``.

The unit tests show the filter in the SQL. Only the warehouse shows which rows
the filter keeps: the comparison of a TIMESTAMP_NTZ column and of a DATE column
with the boundaries, and that the window keeps its start and not its end.

The seed holds four events. The window is 2024-07-01/2024-07-08:

* id 1 at 2024-06-30 23:59:59, before the window.
* id 2 at 2024-07-01 00:00:00, the start. The window keeps it.
* id 3 at 2024-07-07 23:59:59, the last second of the window.
* id 4 at 2024-07-08 00:00:00, the end. The window does not keep it.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

import clair
from clair import SampleWindow
from clair.adapters.snowflake import SnowflakeAdapter
from clair.core.runner import RunSummary
from clair.environments.environments import Environment
from tests.integration.config import IntegrationConfig
from tests.integration.projects import physical_address, write_refined_project
from tests.integration.warehouse import query_rows

pytestmark = pytest.mark.integration

DATABASE_NAME = "sample_window"
WINDOW = SampleWindow(
    start=datetime(2024, 7, 1, tzinfo=UTC), end=datetime(2024, 7, 8, tzinfo=UTC)
)
IDS_IN_THE_WINDOW = [2, 3]
EACH_ID = [1, 2, 3, 4]

EVENTS_FILE = """\
import pandas as pd

from clair import SeedTable

events = pd.DataFrame(
    {
        "id": [1, 2, 3, 4],
        "event_at": pd.to_datetime(
            [
                "2024-06-30 23:59:59",
                "2024-07-01 00:00:00",
                "2024-07-07 23:59:59",
                "2024-07-08 00:00:00",
            ]
        ),
    }
)

trouve = SeedTable(dataframe=events, event_time="event_at")
"""


def table_file(sql: str, event_time: str | None = None) -> str:
    """Give a Table that reads the events. *sql* is the body of the inner f-string."""
    event_time_argument = f'    event_time="{event_time}",\n' if event_time else ""
    return (
        f"from {DATABASE_NAME}.refined.events import trouve as events\n"
        f"from {DATABASE_NAME}.refined.event_days import trouve as event_days\n"
        "from clair import Table\n"
        "trouve = Table(\n"
        f"{event_time_argument}"
        f'    sql=f"{sql}",\n'
        ")\n"
    )


FRAME_IDS_FILE = f"""\
import pandas as pd

from {DATABASE_NAME}.refined.events import trouve as events
from clair import PandasTable


def ids_of(events: pd.DataFrame) -> pd.DataFrame:
    return events[["id"]]


trouve = PandasTable(transform=ids_of, inputs=[events])
"""

TROUVE_FILES = {
    "events": EVENTS_FILE,
    # A DATE column, which the run builds before a reader filters it.
    "event_days": (
        f"from {DATABASE_NAME}.refined.events import trouve as events\n"
        "from clair import Table\n"
        "trouve = Table(\n"
        '    event_time="event_day",\n'
        '    sql=f"SELECT id, TO_DATE(event_at) AS event_day FROM {events.unsampled()}",\n'
        ")\n"
    ),
    "timestamp_ids": table_file("SELECT id FROM {events}"),
    "date_ids": table_file("SELECT id FROM {event_days}"),
    "unsampled_ids": table_file("SELECT id FROM {events.unsampled()}"),
    "frame_ids": FRAME_IDS_FILE,
}


class TestASampledRun:
    @pytest.fixture(scope="class")
    def summary(
        self, environment: Environment, tmp_path_factory: pytest.TempPathFactory
    ) -> RunSummary:
        project_path = write_refined_project(
            tmp_path_factory.mktemp(DATABASE_NAME), DATABASE_NAME, TROUVE_FILES
        )
        return clair.run(project_path, env=environment, sample=WINDOW)

    def ids_of(
        self, adapter: SnowflakeAdapter, clair_environment: IntegrationConfig, table_name: str
    ) -> list[object]:
        address = physical_address(
            f"{DATABASE_NAME}.refined.{table_name}", clair_environment.schema_name
        )
        return [row[0] for row in query_rows(adapter, f"SELECT id FROM {address} ORDER BY id")]

    def test_each_trouve_succeeds(self, summary: RunSummary) -> None:
        assert summary.failed == [], [result.error for result in summary.failed]
        assert summary.succeeded_count == len(TROUVE_FILES)

    @pytest.mark.parametrize(
        ("table_name", "expected_ids"),
        [
            # The seed reads no input, thus the sample keeps each of its rows.
            ("events", EACH_ID),
            ("timestamp_ids", IDS_IN_THE_WINDOW),
            ("date_ids", IDS_IN_THE_WINDOW),
            ("unsampled_ids", EACH_ID),
            ("frame_ids", IDS_IN_THE_WINDOW),
        ],
    )
    def test_the_window_keeps_its_start_and_not_its_end(
        self,
        summary: RunSummary,
        adapter: SnowflakeAdapter,
        clair_environment: IntegrationConfig,
        table_name: str,
        expected_ids: list[int],
    ) -> None:
        assert summary.failed == []
        assert self.ids_of(adapter, clair_environment, table_name) == expected_ids
