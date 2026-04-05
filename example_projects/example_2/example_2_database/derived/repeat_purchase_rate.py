from clair import Column, ColumnType, Trouve, TrouveType
from example_2_database.refined.orders import trouve as example_2_database_refined_orders
from example_2_database.derived.user_order_stats import trouve as example_2_database_derived_user_order_stats

trouve = Trouve(
    type=TrouveType.TABLE,
    docs="Monthly repeat purchase rate among buyers.",
    sql=f"""
        select
            o.order_month,
            count(distinct o.user_id)                                                   as total_buyers,
            count(distinct iff(s.total_orders > 1, o.user_id, null))                    as repeat_buyers,
            div0(
                count(distinct iff(s.total_orders > 1, o.user_id, null)),
                count(distinct o.user_id)
            )                                                                           as repeat_purchase_rate
        from {example_2_database_refined_orders} o
        join {example_2_database_derived_user_order_stats} s
            on o.user_id = s.user_id
        group by 1
    """,
    columns=[
        Column(name="order_month", type=ColumnType.DATE),
        Column(name="total_buyers", type=ColumnType.NUMBER),
        Column(name="repeat_buyers", type=ColumnType.NUMBER),
        Column(name="repeat_purchase_rate", type=ColumnType.FLOAT),
    ],
)
