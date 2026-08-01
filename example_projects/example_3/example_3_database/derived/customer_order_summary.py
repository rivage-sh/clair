from example_3_database.refined.orders import trouve as refined_orders

from clair import (
    Column,
    ColumnType,
    IncrementalMode,
    RunConfig,
    RunMode,
    Trouve,
    TrouveType,
)

trouve = Trouve(
    type=TrouveType.TABLE,
    docs="""
        Order statistics for each customer. Clair merges the rows on customer_id.

        In each incremental run, clair calculates the statistics again from all the refined
        orders and merges them into the target table. Clair updates the customers that exist
        and inserts the new customers. Thus the table always shows the current totals, and
        clair does not make the full table again.

        On full refresh, the table is created from scratch with CREATE OR REPLACE.
    """,
    sql=f"""
        select
            customer_id,
            count(*)                as total_orders,
            sum(amount)             as total_amount,
            min(created_at)         as first_order_at,
            max(created_at)         as last_order_at,
            max(updated_at)         as last_updated_at
        from {refined_orders}
        group by customer_id
    """,
    run_config=RunConfig(
        run_mode=RunMode.INCREMENTAL,
        incremental_mode=IncrementalMode.UPSERT,
        primary_key_columns=["customer_id"],
    ),
    columns=[
        Column(name="customer_id", type=ColumnType.STRING),
        Column(name="total_orders", type=ColumnType.NUMBER),
        Column(name="total_amount", type=ColumnType.FLOAT),
        Column(name="first_order_at", type=ColumnType.TIMESTAMP_NTZ),
        Column(name="last_order_at", type=ColumnType.TIMESTAMP_NTZ),
        Column(name="last_updated_at", type=ColumnType.TIMESTAMP_NTZ),
    ],
)
