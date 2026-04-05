from clair import Column, ColumnType, Trouve, TrouveType

trouve = Trouve(
    type=TrouveType.SOURCE,
    docs="Raw users table from the e-commerce platform.",
    columns=[
        Column(name="user_id", type=ColumnType.STRING),
        Column(name="email", type=ColumnType.STRING),
        Column(name="first_name", type=ColumnType.STRING),
        Column(name="last_name", type=ColumnType.STRING),
        Column(name="created_at", type=ColumnType.TIMESTAMP_NTZ),
        Column(name="address", type=ColumnType.VARIANT),
        Column(name="is_prime_member", type=ColumnType.BOOLEAN),
    ],
)
