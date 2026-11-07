from clair import THIS, Column, ColumnType, Source

trouve = Source(
    docs="An events table that exists before clair runs. It holds the raw user interactions.",
    # `clair run --sample` reads the events of two users, and not a row limit. Each
    # Trouve downstream then holds the complete history of those two users.
    sample_sql=f"SELECT * FROM {THIS} WHERE user_id IN ('usr_abc', 'usr_def')",
    columns=[
        Column(name="event_id", type=ColumnType.STRING),
        Column(name="user_id", type=ColumnType.STRING),
        Column(name="event_type", type=ColumnType.STRING),
        Column(name="occurred_at", type=ColumnType.TIMESTAMP_NTZ),
        Column(name="properties", type=ColumnType.VARIANT),
    ],
)
