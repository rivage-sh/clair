from clair import Column, ColumnType, Source

trouve = Source(
    docs="Raw orders table from the transactional database.",
    columns=[
        Column(name="id", type=ColumnType.STRING),
        Column(name="user_id", type=ColumnType.STRING),
        Column(name="created_at", type=ColumnType.TIMESTAMP),
        Column(name="amount", type=ColumnType.FLOAT),
    ],
)
