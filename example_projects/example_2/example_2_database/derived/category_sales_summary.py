from clair import Column, ColumnType, Trouve, TrouveType
from example_2_database.refined.order_items import trouve as example_2_database_refined_order_items
from example_2_database.refined.orders import trouve as example_2_database_refined_orders
from example_2_database.refined.products import trouve as example_2_database_refined_products

trouve = Trouve(
    type=TrouveType.TABLE,
    docs="Monthly category-level sales aggregates.",
    sql=f"""
        select
            p.category,
            o.order_month,
            count(distinct o.order_id)      as order_count,
            sum(oi.quantity)                 as units_sold,
            sum(oi.net_total)               as net_revenue
        from {example_2_database_refined_order_items} oi
        join {example_2_database_refined_orders} o
            on oi.order_id = o.order_id
        join {example_2_database_refined_products} p
            on oi.product_id = p.product_id
        group by 1, 2
    """,
    columns=[
        Column(name="category", type=ColumnType.STRING),
        Column(name="order_month", type=ColumnType.DATE),
        Column(name="order_count", type=ColumnType.NUMBER),
        Column(name="units_sold", type=ColumnType.NUMBER),
        Column(name="net_revenue", type=ColumnType.FLOAT),
    ],
)
