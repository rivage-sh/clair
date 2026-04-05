from clair import Column, ColumnType, Trouve, TrouveType
from example_1_database.refined.events import trouve as example_1_database_refined_events

trouve = Trouve(
    type=TrouveType.TABLE,
    docs="Daily counts of each event type, aggregated from refined events.",
    sql=f"""
        select
            event_date,
            event_type,
            count(*) as event_count
        from {example_1_database_refined_events}
        group by 1, 2
    """,
    columns=[
        Column(name="event_date", type=ColumnType.DATE),
        Column(name="event_type", type=ColumnType.STRING),
        Column(name="event_count", type=ColumnType.NUMBER),
    ],
)
