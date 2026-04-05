from clair import Column, ColumnType, Trouve, TrouveType
from example_2_database.source.promotions import trouve as example_2_database_source_promotions

trouve = Trouve(
    type=TrouveType.TABLE,
    docs="Refined promotions with flattened rules and active flag.",
    sql=f"""
        select
            promotion_id,
            code,
            discount_type,
            discount_value,
            rules:min_order_value::float         as min_order_value,
            rules:max_uses::number               as max_uses,
            starts_at,
            ends_at,
            current_timestamp() between starts_at and ends_at as is_active
        from {example_2_database_source_promotions}
    """,
    columns=[
        Column(name="promotion_id", type=ColumnType.STRING),
        Column(name="code", type=ColumnType.STRING),
        Column(name="discount_type", type=ColumnType.STRING),
        Column(name="discount_value", type=ColumnType.FLOAT),
        Column(name="min_order_value", type=ColumnType.FLOAT, nullable=True),
        Column(name="max_uses", type=ColumnType.NUMBER, nullable=True),
        Column(name="starts_at", type=ColumnType.TIMESTAMP_NTZ),
        Column(name="ends_at", type=ColumnType.TIMESTAMP_NTZ),
        Column(name="is_active", type=ColumnType.BOOLEAN),
    ],
)
