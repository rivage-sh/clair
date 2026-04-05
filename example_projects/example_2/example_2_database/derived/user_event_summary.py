from clair import Column, ColumnType, Trouve, TrouveType
from example_2_database.refined.events import trouve as example_2_database_refined_events

trouve = Trouve(
    type=TrouveType.TABLE,
    docs="Per-user event engagement summary.",
    sql=f"""
        select
            user_id,
            count(*)                                    as total_events,
            count(distinct event_date)                  as active_days,
            count_if(event_type = 'page_view')          as page_views,
            count_if(event_type = 'product_view')       as product_views,
            count_if(event_type = 'add_to_cart')        as add_to_cart_events,
            count_if(event_type = 'purchase')           as purchase_events,
            min(occurred_at)                            as first_seen_at,
            max(occurred_at)                            as last_seen_at
        from {example_2_database_refined_events}
        group by 1
    """,
    columns=[
        Column(name="user_id", type=ColumnType.STRING),
        Column(name="total_events", type=ColumnType.NUMBER),
        Column(name="active_days", type=ColumnType.NUMBER),
        Column(name="page_views", type=ColumnType.NUMBER),
        Column(name="product_views", type=ColumnType.NUMBER),
        Column(name="add_to_cart_events", type=ColumnType.NUMBER),
        Column(name="purchase_events", type=ColumnType.NUMBER),
        Column(name="first_seen_at", type=ColumnType.TIMESTAMP_NTZ),
        Column(name="last_seen_at", type=ColumnType.TIMESTAMP_NTZ),
    ],
)
