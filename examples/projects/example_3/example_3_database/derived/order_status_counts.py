from _audited_table import AuditedTable
from example_3_database.refined.orders import trouve as refined_orders

from clair import Column, ColumnType, TestNotNull, TestUnique

trouve = AuditedTable(
    docs="""
        The number of orders in each status. Each run keeps its tested candidate as a snapshot.

        AuditedTable renames the promoted staging table to order_status_counts__audit_<run_id>,
        and does not drop it. Thus each run leaves a copy of the data that passed its tests.
    """,
    sql=f"""
        select
            order_status,
            count(*) as order_count
        from {refined_orders}
        group by order_status
    """,
    columns=[
        Column(name="order_status", type=ColumnType.STRING),
        Column(name="order_count", type=ColumnType.NUMBER),
    ],
    tests=[TestUnique(column="order_status"), TestNotNull(column="order_status")],
)
