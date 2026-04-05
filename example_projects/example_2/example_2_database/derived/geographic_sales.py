from clair import Column, ColumnType, Trouve, TrouveType
from example_2_database.refined.orders import trouve as example_2_database_refined_orders
from example_2_database.refined.users import trouve as example_2_database_refined_users

trouve = Trouve(
    type=TrouveType.TABLE,
    docs="Sales aggregates by country and state.",
    sql=f"""
        select
            u.country,
            u.state,
            count(*)                        as order_count,
            count(distinct o.user_id)       as unique_buyers,
            sum(o.total_amount)             as gross_revenue
        from {example_2_database_refined_orders} o
        join {example_2_database_refined_users} u
            on o.user_id = u.user_id
        group by 1, 2
    """,
    columns=[
        Column(name="country", type=ColumnType.STRING),
        Column(name="state", type=ColumnType.STRING),
        Column(name="order_count", type=ColumnType.NUMBER),
        Column(name="unique_buyers", type=ColumnType.NUMBER),
        Column(name="gross_revenue", type=ColumnType.FLOAT),
    ],
)
