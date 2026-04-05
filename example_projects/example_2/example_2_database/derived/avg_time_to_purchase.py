from clair import Column, ColumnType, Trouve, TrouveType
from example_2_database.refined.events import trouve as example_2_database_refined_events
from example_2_database.refined.orders import trouve as example_2_database_refined_orders

trouve = Trouve(
    type=TrouveType.TABLE,
    docs="Per-user time from first event to first purchase.",
    sql=f"""
        select
            e.user_id,
            min(e.occurred_at)                                          as first_event_at,
            min(o.created_at)                                           as first_purchase_at,
            datediff('hour', min(e.occurred_at), min(o.created_at))     as hours_to_first_purchase
        from {example_2_database_refined_events} e
        join {example_2_database_refined_orders} o
            on e.user_id = o.user_id
        group by 1
    """,
    columns=[
        Column(name="user_id", type=ColumnType.STRING),
        Column(name="first_event_at", type=ColumnType.TIMESTAMP_NTZ),
        Column(name="first_purchase_at", type=ColumnType.TIMESTAMP_NTZ),
        Column(name="hours_to_first_purchase", type=ColumnType.NUMBER),
    ],
)
