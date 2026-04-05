from clair import Column, ColumnType, Trouve, TrouveType
from example_2_database.refined.events import trouve as example_2_database_refined_events

trouve = Trouve(
    type=TrouveType.TABLE,
    docs="Daily event counts by type with unique user counts.",
    sql=f"""
        select
            event_date,
            event_type,
            count(*)                        as event_count,
            count(distinct user_id)         as unique_users
        from {example_2_database_refined_events}
        group by 1, 2
    """,
    columns=[
        Column(name="event_date", type=ColumnType.DATE),
        Column(name="event_type", type=ColumnType.STRING),
        Column(name="event_count", type=ColumnType.NUMBER),
        Column(name="unique_users", type=ColumnType.NUMBER),
    ],
)
