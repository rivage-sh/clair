from clair import Column, ColumnType, TestUnique, Trouve, TrouveType
from example_1_database.derived.daily_event_counts import trouve as example_1_database_derived_daily_event_counts

trouve = Trouve(
    type=TrouveType.TABLE,
    docs="Top 10 event types ranked by total count across all days.",
    sql=f"""
        select
            event_type,
            sum(event_count) as total_count
        from {example_1_database_derived_daily_event_counts}
        group by 1
    """,
    columns=[
        Column(name="event_type", type=ColumnType.STRING),
        Column(name="total_count", type=ColumnType.NUMBER),
    ],
    tests=[
        TestUnique(column="event_type"),
    ],
)
