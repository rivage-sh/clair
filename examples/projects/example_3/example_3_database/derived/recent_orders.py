from example_3_database.refined.orders import trouve as refined_orders

import clair
from clair import THIS, Append, Column, ColumnType, Incremental, RunMode, Table

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
    sql += f"""
        where created_at > (select max(created_at) from {THIS})
    """

trouve = Table(
    docs="""
        Incremental append of the orders. In each incremental run, clair adds each order
        that is newer than the newest order of the table. Thus each order arrives one time.

        The filter is a strict greater than, with no tolerance for late rows. Append keeps
        each row that it inserts, thus an overlap would insert a row two times. In a full
        refresh, clair selects all rows and uses no filter.
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
