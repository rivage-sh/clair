from clair import Column, ColumnType, Trouve, TrouveType
from example_2_database.source.inventory import trouve as example_2_database_source_inventory

trouve = Trouve(
    type=TrouveType.TABLE,
    docs="Refined inventory with stock status classification.",
    sql=f"""
        select
            inventory_id,
            product_id,
            warehouse_id,
            quantity_on_hand,
            reorder_threshold,
            last_updated_at,
            case
                when quantity_on_hand = 0                    then 'out_of_stock'
                when quantity_on_hand <= reorder_threshold    then 'low_stock'
                else 'in_stock'
            end as stock_status
        from {example_2_database_source_inventory}
    """,
    columns=[
        Column(name="inventory_id", type=ColumnType.STRING),
        Column(name="product_id", type=ColumnType.STRING),
        Column(name="warehouse_id", type=ColumnType.STRING),
        Column(name="quantity_on_hand", type=ColumnType.NUMBER),
        Column(name="reorder_threshold", type=ColumnType.NUMBER),
        Column(name="last_updated_at", type=ColumnType.TIMESTAMP_NTZ),
        Column(name="stock_status", type=ColumnType.STRING),
    ],
)
