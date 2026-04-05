from clair import Column, ColumnType, Trouve, TrouveType
from example_2_database.derived.product_sales_summary import trouve as example_2_database_derived_product_sales_summary
from example_2_database.refined.products import trouve as example_2_database_refined_products

trouve = Trouve(
    type=TrouveType.TABLE,
    docs="Seller sales breakdown by product category.",
    sql=f"""
        select
            ps.seller_id,
            p.category,
            sum(ps.units_sold)      as units_sold,
            sum(ps.net_revenue)     as net_revenue,
            sum(ps.order_count)     as order_count
        from {example_2_database_derived_product_sales_summary} ps
        join {example_2_database_refined_products} p
            on ps.product_id = p.product_id
        group by 1, 2
    """,
    columns=[
        Column(name="seller_id", type=ColumnType.STRING),
        Column(name="category", type=ColumnType.STRING),
        Column(name="units_sold", type=ColumnType.NUMBER),
        Column(name="net_revenue", type=ColumnType.FLOAT),
        Column(name="order_count", type=ColumnType.NUMBER),
    ],
)
