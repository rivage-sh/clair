from clair import Column, ColumnType, Trouve, TrouveType
from example_2_database.refined.orders import trouve as example_2_database_refined_orders

trouve = Trouve(
    type=TrouveType.TABLE,
    docs="Daily order aggregates including revenue and buyer counts.",
    sql=f"""
        select
            order_date,
            count(*)                        as order_count,
            count(distinct user_id)         as unique_buyers,
            sum(total_amount)               as gross_revenue,
            avg(total_amount)               as avg_order_value
        from {example_2_database_refined_orders}
        group by 1
    """,
    columns=[
        Column(name="order_date", type=ColumnType.DATE),
        Column(name="order_count", type=ColumnType.NUMBER),
        Column(name="unique_buyers", type=ColumnType.NUMBER),
        Column(name="gross_revenue", type=ColumnType.FLOAT),
        Column(name="avg_order_value", type=ColumnType.FLOAT),
    ],
)
