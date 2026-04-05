from clair import Column, ColumnType, Trouve, TrouveType
from example_2_database.derived.seller_performance import trouve as example_2_database_derived_seller_performance

trouve = Trouve(
    type=TrouveType.TABLE,
    docs="Top sellers by total revenue at or above the 90th percentile.",
    sql=f"""
        with p90 as (
            select percentile_cont(0.9) within group (order by total_revenue) as threshold
            from {example_2_database_derived_seller_performance}
        )
        select
            sp.seller_id,
            sp.seller_name,
            sp.is_verified,
            sp.product_count,
            sp.total_units_sold,
            sp.total_revenue,
            sp.avg_product_rating
        from {example_2_database_derived_seller_performance} sp
        cross join p90
        where sp.total_revenue >= p90.threshold
        order by sp.total_revenue desc
    """,
    columns=[
        Column(name="seller_id", type=ColumnType.STRING),
        Column(name="seller_name", type=ColumnType.STRING),
        Column(name="is_verified", type=ColumnType.BOOLEAN),
        Column(name="product_count", type=ColumnType.NUMBER),
        Column(name="total_units_sold", type=ColumnType.NUMBER),
        Column(name="total_revenue", type=ColumnType.FLOAT),
        Column(name="avg_product_rating", type=ColumnType.FLOAT, nullable=True),
    ],
)
