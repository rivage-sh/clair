from clair import Column, ColumnType, Trouve, TrouveType
from example_2_database.source.events import trouve as example_2_database_source_events

trouve = Trouve(
    type=TrouveType.TABLE,
    docs="Refined events with flattened properties and event date.",
    sql=f"""
        select
            event_id,
            user_id,
            event_type,
            occurred_at,
            occurred_at::date                    as event_date,
            properties:page::string              as page,
            properties:referrer::string          as referrer,
            properties:product_id::string        as product_id,
            properties:search_query::string      as search_query,
            properties:cart_value::float         as cart_value,
            properties:element::string           as element
        from {example_2_database_source_events}
    """,
    columns=[
        Column(name="event_id", type=ColumnType.STRING),
        Column(name="user_id", type=ColumnType.STRING),
        Column(name="event_type", type=ColumnType.STRING),
        Column(name="occurred_at", type=ColumnType.TIMESTAMP_NTZ),
        Column(name="event_date", type=ColumnType.DATE),
        Column(name="page", type=ColumnType.STRING, nullable=True),
        Column(name="referrer", type=ColumnType.STRING, nullable=True),
        Column(name="product_id", type=ColumnType.STRING, nullable=True),
        Column(name="search_query", type=ColumnType.STRING, nullable=True),
        Column(name="cart_value", type=ColumnType.FLOAT, nullable=True),
        Column(name="element", type=ColumnType.STRING, nullable=True),
    ],
)
