from clair import Column, ColumnType, Trouve, TrouveType
from example_2_database.refined.sellers import trouve as example_2_database_refined_sellers
from example_2_database.refined.products import trouve as example_2_database_refined_products
from example_2_database.derived.product_sales_summary import trouve as example_2_database_derived_product_sales_summary
from example_2_database.derived.product_review_summary import trouve as example_2_database_derived_product_review_summary

trouve = Trouve(
    type=TrouveType.TABLE,
    docs="Seller-level performance combining sales and review metrics.",
    sql=f"""
        select
            s.seller_id,
            s.name                              as seller_name,
            s.is_verified,
            count(distinct p.product_id)        as product_count,
            coalesce(sum(ps.units_sold), 0)     as total_units_sold,
            coalesce(sum(ps.net_revenue), 0)    as total_revenue,
            avg(pr.avg_rating)                  as avg_product_rating
        from {example_2_database_refined_sellers} s
        join {example_2_database_refined_products} p
            on s.seller_id = p.seller_id
        left join {example_2_database_derived_product_sales_summary} ps
            on p.product_id = ps.product_id
        left join {example_2_database_derived_product_review_summary} pr
            on p.product_id = pr.product_id
        group by 1, 2, 3
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
