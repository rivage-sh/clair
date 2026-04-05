from clair import Column, ColumnType, Trouve, TrouveType

trouve = Trouve(
    type=TrouveType.SOURCE,
    docs="Raw product returns and refund requests.",
    columns=[
        Column(name="return_id", type=ColumnType.STRING),
        Column(name="order_item_id", type=ColumnType.STRING),
        Column(name="user_id", type=ColumnType.STRING),
        Column(name="reason", type=ColumnType.STRING),
        Column(name="status", type=ColumnType.STRING),
        Column(name="created_at", type=ColumnType.TIMESTAMP_NTZ),
        Column(name="refund_amount", type=ColumnType.FLOAT),
    ],
)
