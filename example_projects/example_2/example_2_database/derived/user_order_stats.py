from clair import Column, ColumnType, Trouve, TrouveType
from example_2_database.refined.orders import trouve as example_2_database_refined_orders

trouve = Trouve(
    type=TrouveType.TABLE,
    docs="Per-user lifetime order statistics.",
    sql=f"""
        select
            user_id,
            count(*)                                            as total_orders,
            sum(total_amount)                                   as lifetime_value,
            avg(total_amount)                                   as avg_order_value,
            min(order_date)                                     as first_order_date,
            max(order_date)                                     as last_order_date,
            datediff('day', min(order_date), max(order_date))   as customer_lifespan_days
        from {example_2_database_refined_orders}
        group by 1
    """,
    columns=[
        Column(name="user_id", type=ColumnType.STRING),
        Column(name="total_orders", type=ColumnType.NUMBER),
        Column(name="lifetime_value", type=ColumnType.FLOAT),
        Column(name="avg_order_value", type=ColumnType.FLOAT),
        Column(name="first_order_date", type=ColumnType.DATE),
        Column(name="last_order_date", type=ColumnType.DATE),
        Column(name="customer_lifespan_days", type=ColumnType.NUMBER),
    ],
)
