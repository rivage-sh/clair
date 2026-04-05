from clair import Column, ColumnType, Trouve, TrouveType
from example_2_database.refined.orders import trouve as example_2_database_refined_orders
from example_2_database.derived.user_order_stats import trouve as example_2_database_derived_user_order_stats

trouve = Trouve(
    type=TrouveType.TABLE,
    docs="Daily breakdown of new versus returning buyers.",
    sql=f"""
        select
            o.order_date,
            count(distinct iff(o.order_date = s.first_order_date, o.user_id, null))  as new_buyers,
            count(distinct iff(o.order_date > s.first_order_date, o.user_id, null))  as returning_buyers
        from {example_2_database_refined_orders} o
        join {example_2_database_derived_user_order_stats} s
            on o.user_id = s.user_id
        group by 1
    """,
    columns=[
        Column(name="order_date", type=ColumnType.DATE),
        Column(name="new_buyers", type=ColumnType.NUMBER),
        Column(name="returning_buyers", type=ColumnType.NUMBER),
    ],
)
