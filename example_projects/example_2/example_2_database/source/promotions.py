from clair import Column, ColumnType, Trouve, TrouveType

trouve = Trouve(
    type=TrouveType.SOURCE,
    docs="Raw promotions and discount codes.",
    columns=[
        Column(name="promotion_id", type=ColumnType.STRING),
        Column(name="code", type=ColumnType.STRING),
        Column(name="discount_type", type=ColumnType.STRING),
        Column(name="discount_value", type=ColumnType.FLOAT),
        Column(name="rules", type=ColumnType.VARIANT),
        Column(name="starts_at", type=ColumnType.TIMESTAMP_NTZ),
        Column(name="ends_at", type=ColumnType.TIMESTAMP_NTZ),
    ],
)
