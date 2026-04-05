from clair import Column, ColumnType, Trouve, TrouveType

trouve = Trouve(
    type=TrouveType.SOURCE,
    docs="Raw inventory levels per product and warehouse.",
    columns=[
        Column(name="inventory_id", type=ColumnType.STRING),
        Column(name="product_id", type=ColumnType.STRING),
        Column(name="warehouse_id", type=ColumnType.STRING),
        Column(name="quantity_on_hand", type=ColumnType.NUMBER),
        Column(name="reorder_threshold", type=ColumnType.NUMBER),
        Column(name="last_updated_at", type=ColumnType.TIMESTAMP_NTZ),
    ],
)
