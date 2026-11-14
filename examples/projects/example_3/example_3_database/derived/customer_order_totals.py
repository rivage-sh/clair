from _mirrored_upsert_table import MirroredUpsertTable
from example_3_database.refined.orders import trouve as refined_orders

from clair import Column, ColumnType, Incremental, TestUnique, Upsert

trouve = MirroredUpsertTable(
    docs="""
        The totals of each customer, merged in place. A stream on this table keeps reading.

        clair tests the MERGE on a clone, as for each Table. MirroredUpsertTable then runs the
        same MERGE into this table, and does not replace it. Thus the table keeps its Time
        Travel history, and a stream or a dynamic table on it keeps working.
    """,
    sql=f"""
        select
            customer_id,
            count(*)    as total_orders,
            sum(amount) as total_amount
        from {refined_orders}
        group by customer_id
    """,
    run_config=Incremental(strategy=Upsert(primary_key_columns=["customer_id"])),
    columns=[
        Column(name="customer_id", type=ColumnType.STRING),
        Column(name="total_orders", type=ColumnType.NUMBER),
        Column(name="total_amount", type=ColumnType.FLOAT),
    ],
    tests=[TestUnique(column="customer_id")],
)
