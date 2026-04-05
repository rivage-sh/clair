from clair import Column, ColumnType, Trouve, TrouveType
from example_1_database.source.events import trouve as example_1_database_source_events

trouve = Trouve(
    type=TrouveType.TABLE,
    docs="""
        Refined layer: flattens the semi-structured PROPERTIES VARIANT into typed columns
        and adds convenience fields. Downstream trouves reference this instead of the raw source.
    """,
    sql=f"""
        select
            event_id,
            user_id,
            event_type,
            occurred_at,
            occurred_at::date                   as event_date,

            -- page_view / button_click
            properties:page::string             as page,
            properties:referrer::string         as referrer,

            -- button_click
            properties:element::string          as element,

            -- form_submit
            properties:form::string             as form,
            properties:success::boolean         as form_success,

            -- purchase
            properties:amount::float            as purchase_amount,
            properties:currency::string         as purchase_currency,
            properties:item_id::string          as purchase_item_id
        from {example_1_database_source_events}
    """,
    columns=[
        Column(name="event_id", type=ColumnType.STRING),
        Column(name="user_id", type=ColumnType.STRING),
        Column(name="event_type", type=ColumnType.STRING),
        Column(name="occurred_at", type=ColumnType.TIMESTAMP_NTZ),
        Column(name="event_date", type=ColumnType.DATE),
        Column(name="page", type=ColumnType.STRING, nullable=True),
        Column(name="referrer", type=ColumnType.STRING, nullable=True),
        Column(name="element", type=ColumnType.STRING, nullable=True),
        Column(name="form", type=ColumnType.STRING, nullable=True),
        Column(name="form_success", type=ColumnType.BOOLEAN, nullable=True),
        Column(name="purchase_amount", type=ColumnType.FLOAT, nullable=True),
        Column(name="purchase_currency", type=ColumnType.STRING, nullable=True),
        Column(name="purchase_item_id", type=ColumnType.STRING, nullable=True),
    ],
)
