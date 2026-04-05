from clair import Column, ColumnType, Trouve, TrouveType

trouve = Trouve(
    type=TrouveType.SOURCE,
    docs="Raw order line items linking orders to products.",
    columns=[
        Column(name="order_item_id", type=ColumnType.STRING),
        Column(name="order_id", type=ColumnType.STRING),
        Column(name="product_id", type=ColumnType.STRING),
        Column(name="quantity", type=ColumnType.NUMBER),
        Column(name="unit_price", type=ColumnType.FLOAT),
        Column(name="discount_amount", type=ColumnType.FLOAT, nullable=True),
    ],
)
