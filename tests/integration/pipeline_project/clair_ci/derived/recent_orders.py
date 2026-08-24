from clair_ci.refined.orders import trouve as clair_ci_refined_orders

import clair
from clair import (
    Column,
    ColumnType,
    IncrementalMode,
    RunConfig,
    RunMode,
    Trouve,
    TrouveType,
)

sql = f"""
    select
        order_id,
        user_id,
        order_status,
        amount,
        created_at,
        created_date
    from {clair_ci_refined_orders}
"""
if clair.run_mode == RunMode.INCREMENTAL:
    sql += """
        where created_at > dateadd('day', -3, current_timestamp())
    """

trouve = Trouve(
    type=TrouveType.TABLE,
    docs="""
        Incremental append of the recent orders. A full refresh writes every seeded order.
        An incremental run adds the orders of the last 3 days again, thus the row count
        grows by a number that the integration test knows.
    """,
    sql=sql,
    run_config=RunConfig(
        run_mode=RunMode.INCREMENTAL,
        incremental_mode=IncrementalMode.APPEND,
    ),
    columns=[
        Column(name="order_id", type=ColumnType.STRING),
        Column(name="user_id", type=ColumnType.STRING),
        Column(name="order_status", type=ColumnType.STRING),
        Column(name="amount", type=ColumnType.FLOAT),
        Column(name="created_at", type=ColumnType.TIMESTAMP_NTZ),
        Column(name="created_date", type=ColumnType.DATE),
    ],
)
