from clair import Column, ColumnType, Trouve, TrouveType
from example_2_database.refined.orders import trouve as example_2_database_refined_orders

trouve = Trouve(
    type=TrouveType.TABLE,
    docs="Per-user monthly order count and spend.",
    sql=f"""
        select
            user_id,
            order_month,
            count(*)              as order_count,
            sum(total_amount)     as monthly_spend
        from {example_2_database_refined_orders}
        group by 1, 2
    """,
    columns=[
        Column(name="user_id", type=ColumnType.STRING),
        Column(name="order_month", type=ColumnType.DATE),
        Column(name="order_count", type=ColumnType.NUMBER),
        Column(name="monthly_spend", type=ColumnType.FLOAT),
    ],
)
