from example_3_database.refined.orders import trouve as refined_orders

import clair
from clair import Append, Column, ColumnType, Incremental, RunMode, Table

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

trouve = Table(
    docs="""
        Incremental append of the recent orders. In each incremental run, clair adds the
        orders from the last 3 days. Thus the table keeps a log of the recent activity.

        Note: the lookback of 3 days gives a small overlap. The overlap catches the rows that
        come late. In a full refresh, clair selects all rows and uses no date filter.
    """,
    sql=sql,
    run_config=Incremental(strategy=Append()),
    columns=[
        Column(name="order_id", type=ColumnType.STRING),
        Column(name="customer_id", type=ColumnType.STRING),
        Column(name="order_status", type=ColumnType.STRING),
        Column(name="amount", type=ColumnType.FLOAT),
        Column(name="created_at", type=ColumnType.TIMESTAMP_NTZ),
        Column(name="created_date", type=ColumnType.DATE),
    ],
)
