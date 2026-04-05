from clair import Column, ColumnType, Trouve, TrouveType
from example_2_database.refined.order_items import trouve as example_2_database_refined_order_items
from example_2_database.refined.products import trouve as example_2_database_refined_products

trouve = Trouve(
    type=TrouveType.TABLE,
    docs="Per-product sales aggregates joined with product details.",
    sql=f"""
        select
            p.product_id,
            p.title,
            p.category,
            p.seller_id,
            count(distinct oi.order_id)     as order_count,
            sum(oi.quantity)                 as units_sold,
            sum(oi.net_total)               as net_revenue,
            avg(oi.unit_price)              as avg_unit_price
        from {example_2_database_refined_order_items} oi
        join {example_2_database_refined_products} p
            on oi.product_id = p.product_id
        group by 1, 2, 3, 4
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
