from clair import Column, ColumnType, Trouve, TrouveType
from example_2_database.derived.product_sales_summary import trouve as example_2_database_derived_product_sales_summary

trouve = Trouve(
    type=TrouveType.TABLE,
    docs="Underperforming products by net revenue at or below the 10th percentile.",
    sql=f"""
        with p10 as (
            select percentile_cont(0.1) within group (order by net_revenue) as threshold
            from {example_2_database_derived_product_sales_summary}
        )
        select
            ps.product_id,
            ps.title,
            ps.category,
            ps.seller_id,
            ps.order_count,
            ps.units_sold,
            ps.net_revenue,
            ps.avg_unit_price
        from {example_2_database_derived_product_sales_summary} ps
        cross join p10
        where ps.net_revenue <= p10.threshold
        order by ps.net_revenue asc
    """,
    columns=[
        Column(name="product_id", type=ColumnType.STRING),
        Column(name="title", type=ColumnType.STRING),
        Column(name="category", type=ColumnType.STRING),
        Column(name="seller_id", type=ColumnType.STRING),
        Column(name="order_count", type=ColumnType.NUMBER),
        Column(name="units_sold", type=ColumnType.NUMBER),
        Column(name="net_revenue", type=ColumnType.FLOAT),
        Column(name="avg_unit_price", type=ColumnType.FLOAT),
    ],
)
