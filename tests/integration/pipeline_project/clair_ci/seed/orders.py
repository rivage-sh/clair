from clair import Column, ColumnType, Trouve, TrouveType

trouve = Trouve(
    type=TrouveType.SOURCE,
    docs="""
        The seeded orders table. Some rows have a created_at in the last 3 days, and
        the other rows are older. The incremental Trouve downstream selects the recent
        rows only, thus the integration test can count them.
    """,
    columns=[
        Column(name="order_id", type=ColumnType.STRING),
        Column(name="user_id", type=ColumnType.STRING),
        Column(name="order_status", type=ColumnType.STRING),
        Column(name="amount", type=ColumnType.FLOAT),
        Column(name="created_at", type=ColumnType.TIMESTAMP_NTZ),
    ],
)
