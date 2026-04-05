from clair import Column, ColumnType, Trouve, TrouveType

trouve = Trouve(
    type=TrouveType.SOURCE,
    docs="Raw orders table capturing customer purchases.",
    columns=[
        Column(name="order_id", type=ColumnType.STRING),
        Column(name="user_id", type=ColumnType.STRING),
        Column(name="status", type=ColumnType.STRING),
        Column(name="created_at", type=ColumnType.TIMESTAMP_NTZ),
        Column(name="shipped_at", type=ColumnType.TIMESTAMP_NTZ),
        Column(name="delivered_at", type=ColumnType.TIMESTAMP_NTZ),
        Column(name="promotion_id", type=ColumnType.STRING, nullable=True),
        Column(name="total_amount", type=ColumnType.FLOAT),
    ],
)
