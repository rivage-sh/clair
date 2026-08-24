from clair_ci.seed.orders import trouve as clair_ci_seed_orders

from clair import Column, ColumnType, TestNotNull, TestUnique, Trouve, TrouveType

trouve = Trouve(
    type=TrouveType.TABLE,
    docs="The refined orders. It adds the order date.",
    sql=f"""
        select
            order_id,
            user_id,
            order_status,
            amount,
            created_at,
            created_at::date                as created_date
        from {clair_ci_seed_orders}
    """,
    columns=[
        Column(name="order_id", type=ColumnType.STRING),
        Column(name="user_id", type=ColumnType.STRING),
        Column(name="order_status", type=ColumnType.STRING),
        Column(name="amount", type=ColumnType.FLOAT),
        Column(name="created_at", type=ColumnType.TIMESTAMP_NTZ),
        Column(name="created_date", type=ColumnType.DATE),
    ],
    tests=[
        TestUnique(column="order_id"),
        TestNotNull(column="amount"),
    ],
)
