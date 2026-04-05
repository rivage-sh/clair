from clair import Column, ColumnType, Trouve, TrouveType
from example_3_database.source.orders import trouve as source_orders

trouve = Trouve(
    type=TrouveType.TABLE,
    docs="Refined orders: typed, cleaned, and enriched with date columns. Always full refresh.",
    sql=f"""
        select
            order_id,
            customer_id,
            order_status,
            amount,
            created_at,
            updated_at,
            created_at::date as created_date,
            updated_at::date as updated_date
        from {source_orders}
    """,
    columns=[
        Column(name="order_id", type=ColumnType.STRING),
        Column(name="customer_id", type=ColumnType.STRING),
        Column(name="order_status", type=ColumnType.STRING),
        Column(name="amount", type=ColumnType.FLOAT),
        Column(name="created_at", type=ColumnType.TIMESTAMP_NTZ),
        Column(name="updated_at", type=ColumnType.TIMESTAMP_NTZ),
        Column(name="created_date", type=ColumnType.DATE),
        Column(name="updated_date", type=ColumnType.DATE),
    ],
)
