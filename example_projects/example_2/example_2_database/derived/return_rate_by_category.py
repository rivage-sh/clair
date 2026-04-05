from clair import Column, ColumnType, Trouve, TrouveType
from example_2_database.derived.return_rate_by_product import trouve as example_2_database_derived_return_rate_by_product
from example_2_database.refined.products import trouve as example_2_database_refined_products

trouve = Trouve(
    type=TrouveType.TABLE,
    docs="Per-category return rates aggregated from product-level data.",
    sql=f"""
        select
            p.category,
            sum(rr.total_order_items)                               as total_order_items,
            sum(rr.total_returns)                                   as total_returns,
            div0(sum(rr.total_returns), sum(rr.total_order_items))  as return_rate
        from {example_2_database_derived_return_rate_by_product} rr
        join {example_2_database_refined_products} p
            on rr.product_id = p.product_id
        group by 1
    """,
    columns=[
        Column(name="category", type=ColumnType.STRING),
        Column(name="total_order_items", type=ColumnType.NUMBER),
        Column(name="total_returns", type=ColumnType.NUMBER),
        Column(name="return_rate", type=ColumnType.FLOAT),
    ],
)
