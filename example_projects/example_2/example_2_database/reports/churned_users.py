from clair import Column, ColumnType, Trouve, TrouveType
from example_2_database.derived.user_order_stats import trouve as example_2_database_derived_user_order_stats
from example_2_database.refined.users import trouve as example_2_database_refined_users

trouve = Trouve(
    type=TrouveType.TABLE,
    docs="Users with no order in the last 90 days, likely churned.",
    sql=f"""
        select
            s.user_id,
            u.email,
            u.full_name,
            s.last_order_date,
            s.total_orders,
            s.lifetime_value,
            datediff('day', s.last_order_date, current_date()) as days_since_last_order
        from {example_2_database_derived_user_order_stats} s
        join {example_2_database_refined_users} u
            on s.user_id = u.user_id
        where datediff('day', s.last_order_date, current_date()) > 90
        order by days_since_last_order desc
    """,
    columns=[
        Column(name="user_id", type=ColumnType.STRING),
        Column(name="email", type=ColumnType.STRING),
        Column(name="full_name", type=ColumnType.STRING),
        Column(name="last_order_date", type=ColumnType.DATE),
        Column(name="total_orders", type=ColumnType.NUMBER),
        Column(name="lifetime_value", type=ColumnType.FLOAT),
        Column(name="days_since_last_order", type=ColumnType.NUMBER),
    ],
)
