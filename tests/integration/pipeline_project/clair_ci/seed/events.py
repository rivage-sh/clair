from clair import Column, ColumnType, Trouve, TrouveType

trouve = Trouve(
    type=TrouveType.SOURCE,
    docs="""
        The seeded events table. tests/integration/ci_snowflake.py writes it before
        the integration tests start. A SOURCE Trouve never routes, thus this table
        keeps the logical name and every CI run reads the same rows.
    """,
    columns=[
        Column(name="event_id", type=ColumnType.STRING),
        Column(name="user_id", type=ColumnType.STRING),
        Column(name="event_type", type=ColumnType.STRING),
        Column(name="occurred_at", type=ColumnType.TIMESTAMP_NTZ),
        Column(name="properties", type=ColumnType.VARIANT),
    ],
)
