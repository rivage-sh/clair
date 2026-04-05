from clair import Column, ColumnType, Trouve, TrouveType
from source.raw.orders import trouve as raw_orders

trouve = Trouve(
    type=TrouveType.TABLE,
    docs="Daily order totals aggregated from raw orders.",
    sql=f"""
        select
            date_trunc('day', created_at) as order_date,
            count(*)                      as order_count,
            sum(amount)                   as total_amount
        from {raw_orders}
        group by 1
    """,
    columns=[
        Column(name="order_date", type=ColumnType.DATE),
        Column(name="order_count", type=ColumnType.INTEGER),
        Column(name="total_amount", type=ColumnType.FLOAT),
    ],
)
