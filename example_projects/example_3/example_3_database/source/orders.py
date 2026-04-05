from clair import Column, ColumnType, Trouve, TrouveType

trouve = Trouve(
    type=TrouveType.SOURCE,
    docs="Raw orders table. Each row is one order. updated_at changes when an order status changes.",
    columns=[
        Column(name="order_id", type=ColumnType.STRING),
        Column(name="customer_id", type=ColumnType.STRING),
        Column(name="order_status", type=ColumnType.STRING),
        Column(name="amount", type=ColumnType.FLOAT),
        Column(name="created_at", type=ColumnType.TIMESTAMP_NTZ),
        Column(name="updated_at", type=ColumnType.TIMESTAMP_NTZ),
    ],
)
