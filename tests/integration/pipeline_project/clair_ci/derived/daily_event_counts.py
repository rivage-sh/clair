from clair_ci.refined.events import trouve as clair_ci_refined_events

from clair import Column, ColumnType, TestRowCount, Trouve, TrouveType

trouve = Trouve(
    type=TrouveType.TABLE,
    docs="Daily count of each event type. It reads the refined events.",
    sql=f"""
        select
            event_date,
            event_type,
            count(*)                        as event_count
        from {clair_ci_refined_events}
        group by 1, 2
    """,
    columns=[
        Column(name="event_date", type=ColumnType.DATE),
        Column(name="event_type", type=ColumnType.STRING),
        Column(name="event_count", type=ColumnType.NUMBER),
    ],
    tests=[
        TestRowCount(min_rows=1),
    ],
)
