from clair import Column, ColumnType, Trouve, TrouveType
from example_2_database.refined.events import trouve as example_2_database_refined_events

trouve = Trouve(
    type=TrouveType.TABLE,
    docs="Daily conversion funnel from page views to purchases.",
    sql=f"""
        select
            event_date,
            count(distinct iff(event_type = 'page_view',    user_id, null)) as visitors,
            count(distinct iff(event_type = 'product_view', user_id, null)) as product_viewers,
            count(distinct iff(event_type = 'add_to_cart',  user_id, null)) as cart_adders,
            count(distinct iff(event_type = 'purchase',     user_id, null)) as purchasers
        from {example_2_database_refined_events}
        group by 1
    """,
    columns=[
        Column(name="event_date", type=ColumnType.DATE),
        Column(name="visitors", type=ColumnType.NUMBER),
        Column(name="product_viewers", type=ColumnType.NUMBER),
        Column(name="cart_adders", type=ColumnType.NUMBER),
        Column(name="purchasers", type=ColumnType.NUMBER),
    ],
)
