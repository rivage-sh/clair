from example_2_database.reference.country_regions import trouve as example_2_database_reference_country_regions
from example_2_database.refined.orders import trouve as example_2_database_refined_orders
from example_2_database.refined.users import trouve as example_2_database_refined_users

from clair import Column, ColumnType, Trouve, TrouveType

trouve = Trouve(
    type=TrouveType.TABLE,
    docs="Sales totals for each country and each state, with the sales region.",
    sql=f"""
        select
            u.country,
            u.state,
            r.region,
            count(*)                        as order_count,
            count(distinct o.user_id)       as unique_buyers,
            sum(o.total_amount)             as gross_revenue
        from {example_2_database_refined_orders} o
        join {example_2_database_refined_users} u
            on o.user_id = u.user_id
        left join {example_2_database_reference_country_regions} r
            on r.country = u.country
        group by 1, 2, 3
    """,
    columns=[
        Column(name="country", type=ColumnType.STRING),
        Column(name="state", type=ColumnType.STRING),
        Column(name="region", type=ColumnType.STRING),
        Column(name="order_count", type=ColumnType.NUMBER),
        Column(name="unique_buyers", type=ColumnType.NUMBER),
        Column(name="gross_revenue", type=ColumnType.FLOAT),
    ],
)
