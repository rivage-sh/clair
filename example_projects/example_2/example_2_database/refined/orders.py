from clair import Column, ColumnType, Trouve, TrouveType
from example_2_database.source.orders import trouve as example_2_database_source_orders

trouve = Trouve(
    type=TrouveType.TABLE,
    docs="Refined orders with date dimensions and delivery metrics.",
    sql=f"""
        select
            order_id,
            user_id,
            status,
            created_at,
            shipped_at,
            delivered_at,
            promotion_id,
            total_amount,
            created_at::date                                as order_date,
            date_trunc('month', created_at)::date           as order_month,
            datediff('hour', created_at, shipped_at)        as hours_to_ship,
            datediff('hour', shipped_at, delivered_at)      as hours_to_deliver,
            case
                when total_amount < 25  then 'small'
                when total_amount < 100 then 'medium'
                when total_amount < 500 then 'large'
                else 'enterprise'
            end                                             as order_size_tier
        from {example_2_database_source_orders}
    """,
    columns=[
        Column(name="order_id", type=ColumnType.STRING),
        Column(name="user_id", type=ColumnType.STRING),
        Column(name="status", type=ColumnType.STRING),
        Column(name="created_at", type=ColumnType.TIMESTAMP_NTZ),
        Column(name="shipped_at", type=ColumnType.TIMESTAMP_NTZ),
        Column(name="delivered_at", type=ColumnType.TIMESTAMP_NTZ),
        Column(name="promotion_id", type=ColumnType.STRING, nullable=True),
        Column(name="total_amount", type=ColumnType.FLOAT),
        Column(name="order_date", type=ColumnType.DATE),
        Column(name="order_month", type=ColumnType.DATE),
        Column(name="hours_to_ship", type=ColumnType.NUMBER, nullable=True),
        Column(name="hours_to_deliver", type=ColumnType.NUMBER, nullable=True),
        Column(name="order_size_tier", type=ColumnType.STRING),
    ],
)
