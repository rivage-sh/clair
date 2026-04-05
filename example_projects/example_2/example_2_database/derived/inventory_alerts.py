from clair import Column, ColumnType, Trouve, TrouveType
from example_2_database.refined.inventory import trouve as example_2_database_refined_inventory
from example_2_database.refined.products import trouve as example_2_database_refined_products

trouve = Trouve(
    type=TrouveType.TABLE,
    docs="Products with low or out-of-stock inventory levels.",
    sql=f"""
        select
            i.product_id,
            p.title             as product_title,
            p.category,
            i.warehouse_id,
            i.quantity_on_hand,
            i.reorder_threshold,
            i.stock_status,
            i.last_updated_at
        from {example_2_database_refined_inventory} i
        join {example_2_database_refined_products} p
            on i.product_id = p.product_id
        where i.stock_status in ('out_of_stock', 'low_stock')
    """,
    columns=[
        Column(name="product_id", type=ColumnType.STRING),
        Column(name="product_title", type=ColumnType.STRING),
        Column(name="category", type=ColumnType.STRING),
        Column(name="warehouse_id", type=ColumnType.STRING),
        Column(name="quantity_on_hand", type=ColumnType.NUMBER),
        Column(name="reorder_threshold", type=ColumnType.NUMBER),
        Column(name="stock_status", type=ColumnType.STRING),
        Column(name="last_updated_at", type=ColumnType.TIMESTAMP_NTZ),
    ],
)
