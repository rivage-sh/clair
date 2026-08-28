from example_2_database.derived.geographic_sales import trouve as example_2_database_derived_geographic_sales

from clair import Column, ColumnType, Trouve, TrouveType

trouve = Trouve(
    type=TrouveType.TABLE,
    docs="The sales totals of each region, in the sequence of the revenue.",
    sql=f"""
        select
            coalesce(region, 'Unknown')     as region,
            count(distinct country)         as country_count,
            sum(order_count)                as order_count,
            sum(unique_buyers)              as unique_buyers,
            sum(gross_revenue)              as gross_revenue
        from {example_2_database_derived_geographic_sales}
        group by 1
        order by gross_revenue desc
    """,
    columns=[
        Column(name="region", type=ColumnType.STRING),
        Column(name="country_count", type=ColumnType.NUMBER),
        Column(name="order_count", type=ColumnType.NUMBER),
        Column(name="unique_buyers", type=ColumnType.NUMBER),
        Column(name="gross_revenue", type=ColumnType.FLOAT),
    ],
)
