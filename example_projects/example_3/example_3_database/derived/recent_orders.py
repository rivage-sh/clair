import clair
from clair import Column, ColumnType, IncrementalMode, RunConfig, RunMode, Trouve, TrouveType
from example_3_database.refined.orders import trouve as refined_orders

sql = f"""
    select
        order_id,
        customer_id,
        order_status,
        amount,
        created_at,
        created_date
    from {refined_orders}
"""
if clair.run_mode == RunMode.INCREMENTAL:
    sql += """
        where created_at > dateadd('day', -3, current_timestamp())
    """

trouve = Trouve(
    type=TrouveType.TABLE,
    docs="""
        Incremental append of recent orders. On each incremental run, orders created in the
        last 3 days are appended. Intended to accumulate a running log of recent activity.

        Note: the 3-day lookback provides a small overlap window to handle late-arriving rows.
        On full refresh, all rows are selected without a date filter.
    """,
    sql=sql,
    run_config=RunConfig(
        run_mode=RunMode.INCREMENTAL,
        incremental_mode=IncrementalMode.APPEND,
    ),
    columns=[
        Column(name="order_id", type=ColumnType.STRING),
        Column(name="customer_id", type=ColumnType.STRING),
        Column(name="order_status", type=ColumnType.STRING),
        Column(name="amount", type=ColumnType.FLOAT),
        Column(name="created_at", type=ColumnType.TIMESTAMP_NTZ),
        Column(name="created_date", type=ColumnType.DATE),
    ],
)
