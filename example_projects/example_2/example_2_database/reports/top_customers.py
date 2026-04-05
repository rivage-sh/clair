from clair import Column, ColumnType, Trouve, TrouveType
from example_2_database.derived.user_order_stats import trouve as example_2_database_derived_user_order_stats
from example_2_database.refined.users import trouve as example_2_database_refined_users

trouve = Trouve(
    type=TrouveType.TABLE,
    docs="Top customers by lifetime value at or above the 90th percentile.",
    sql=f"""
        with p90 as (
            select percentile_cont(0.9) within group (order by lifetime_value) as threshold
            from {example_2_database_derived_user_order_stats}
        )
        select
            s.user_id,
            u.email,
            u.full_name,
            u.is_prime_member,
            s.total_orders,
            s.lifetime_value,
            s.avg_order_value,
            s.first_order_date,
            s.last_order_date,
            s.customer_lifespan_days
        from {example_2_database_derived_user_order_stats} s
        join {example_2_database_refined_users} u
            on s.user_id = u.user_id
        cross join p90
        where s.lifetime_value >= p90.threshold
        order by s.lifetime_value desc
    """,
    columns=[
        Column(name="user_id", type=ColumnType.STRING),
        Column(name="email", type=ColumnType.STRING),
        Column(name="full_name", type=ColumnType.STRING),
        Column(name="is_prime_member", type=ColumnType.BOOLEAN),
        Column(name="total_orders", type=ColumnType.NUMBER),
        Column(name="lifetime_value", type=ColumnType.FLOAT),
        Column(name="avg_order_value", type=ColumnType.FLOAT),
        Column(name="first_order_date", type=ColumnType.DATE),
        Column(name="last_order_date", type=ColumnType.DATE),
        Column(name="customer_lifespan_days", type=ColumnType.NUMBER),
    ],
)
