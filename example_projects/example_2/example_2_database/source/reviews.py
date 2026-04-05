from clair import Column, ColumnType, Trouve, TrouveType

trouve = Trouve(
    type=TrouveType.SOURCE,
    docs="Raw product reviews submitted by customers.",
    columns=[
        Column(name="review_id", type=ColumnType.STRING),
        Column(name="product_id", type=ColumnType.STRING),
        Column(name="user_id", type=ColumnType.STRING),
        Column(name="order_id", type=ColumnType.STRING),
        Column(name="rating", type=ColumnType.NUMBER),
        Column(name="title", type=ColumnType.STRING),
        Column(name="body", type=ColumnType.STRING),
        Column(name="created_at", type=ColumnType.TIMESTAMP_NTZ),
    ],
)
