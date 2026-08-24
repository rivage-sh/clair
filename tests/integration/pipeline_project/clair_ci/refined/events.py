from clair_ci.seed.events import trouve as clair_ci_seed_events

from clair import Column, ColumnType, TestNotNull, TestRowCount, TestUnique, Trouve, TrouveType

trouve = Trouve(
    type=TrouveType.TABLE,
    docs="The refined events. It puts the PROPERTIES VARIANT into typed columns.",
    sql=f"""
        select
            event_id,
            user_id,
            event_type,
            occurred_at,
            occurred_at::date               as event_date,
            properties:page::string         as page,
            properties:amount::float        as purchase_amount
        from {clair_ci_seed_events}
    """,
    columns=[
        Column(name="event_id", type=ColumnType.STRING),
        Column(name="user_id", type=ColumnType.STRING),
        Column(name="event_type", type=ColumnType.STRING),
        Column(name="occurred_at", type=ColumnType.TIMESTAMP_NTZ),
        Column(name="event_date", type=ColumnType.DATE),
        Column(name="page", type=ColumnType.STRING, nullable=True),
        Column(name="purchase_amount", type=ColumnType.FLOAT, nullable=True),
    ],
    tests=[
        TestUnique(column="event_id"),
        TestNotNull(column="user_id"),
        TestRowCount(min_rows=1),
    ],
)
