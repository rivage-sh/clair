import pandas as pd

from clair import Column, ColumnType, Trouve
from example_4_database.refined.events import trouve as example_4_database_refined_events


def daily_event_counts(
    refined_events: pd.DataFrame = example_4_database_refined_events,  # type: ignore
) -> pd.DataFrame:
    return (
        refined_events
        .groupby(["event_date", "event_type"], as_index=False)
        .size()
        .rename(columns={"size": "event_count"})  # type: ignore
    )


trouve = Trouve(
    df_fn=daily_event_counts,
    docs="Daily counts of each event type, aggregated from refined events.",
    columns=[
        Column(name="event_date", type=ColumnType.DATE),
        Column(name="event_type", type=ColumnType.STRING),
        Column(name="event_count", type=ColumnType.NUMBER),
    ],
)
