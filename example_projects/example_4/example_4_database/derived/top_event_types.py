from clair import Column, ColumnType, Trouve
from example_4_database.derived.daily_event_counts import trouve as example_4_database_derived_daily_event_counts

trouve = Trouve(
    docs="Top 10 event types ranked by total count across all time.",
    sql=f"""
        select
            event_type,
            sum(event_count) as total_event_count
        from {example_4_database_derived_daily_event_counts}
        group by event_type
        order by total_event_count desc
        limit 10
    """,
    columns=[
        Column(name="event_type", type=ColumnType.STRING),
        Column(name="total_event_count", type=ColumnType.NUMBER),
    ],
)
