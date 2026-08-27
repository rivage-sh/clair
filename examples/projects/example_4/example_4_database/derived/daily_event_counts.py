import pandas as pd
from example_4_database.refined.events import trouve as example_4_database_refined_events

from clair import Column, ColumnType, PandasTrouve, TestNotNull, TestUniqueColumns


def daily_event_counts(refined_events: pd.DataFrame) -> pd.DataFrame:
    return refined_events.groupby(
        ["event_date", "event_type"], as_index=False
    ).agg(event_count=("event_type", "size"))


trouve = PandasTrouve(
    transform=daily_event_counts,
    inputs=[example_4_database_refined_events],
    docs="Daily count of each event type. This Trouve reads the refined events.",
    columns=[
        Column(name="event_date", type=ColumnType.DATE),
        Column(name="event_type", type=ColumnType.STRING),
        Column(name="event_count", type=ColumnType.NUMBER),
    ],
    tests=[
        TestUniqueColumns(columns=["event_date", "event_type"]),
        TestNotNull(column="event_count"),
    ],
)
