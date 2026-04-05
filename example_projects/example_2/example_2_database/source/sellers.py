from clair import Column, ColumnType, Trouve, TrouveType

trouve = Trouve(
    type=TrouveType.SOURCE,
    docs="Raw sellers table for marketplace vendors.",
    columns=[
        Column(name="seller_id", type=ColumnType.STRING),
        Column(name="name", type=ColumnType.STRING),
        Column(name="email", type=ColumnType.STRING),
        Column(name="contact_info", type=ColumnType.VARIANT),
        Column(name="joined_at", type=ColumnType.TIMESTAMP_NTZ),
        Column(name="is_verified", type=ColumnType.BOOLEAN),
    ],
)
