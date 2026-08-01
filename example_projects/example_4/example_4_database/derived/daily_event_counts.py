import pandas as pd
from example_4_database.refined.events import trouve as example_4_database_refined_events

from clair import Column, ColumnType, PandasTrouve


def daily_event_counts(refined_events: pd.DataFrame) -> pd.DataFrame:
    return refined_events.groupby(
        ["event_date", "event_type"], as_index=False
    ).agg(event_count=("event_type", "size"))


trouve = PandasTrouve(
    transform=daily_event_counts,
    inputs=[example_4_database_refined_events],
    docs="Daily counts of each event type, aggregated from refined events.",
    columns=[
        Column(name="event_date", type=ColumnType.DATE),
        Column(name="event_type", type=ColumnType.STRING),
        Column(name="event_count", type=ColumnType.NUMBER),
    ],
)
