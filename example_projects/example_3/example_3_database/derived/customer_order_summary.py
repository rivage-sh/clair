from clair import Column, ColumnType, IncrementalMode, RunConfig, RunMode, Trouve, TrouveType
from example_3_database.refined.orders import trouve as refined_orders

trouve = Trouve(
    type=TrouveType.TABLE,
    docs="""
        Per-customer order statistics, merged on customer_id.

        On each incremental run, stats are recomputed from all refined orders and merged
        into the target table. Existing customers are updated in-place; new customers are
        inserted. This means the table always reflects current totals without a full rebuild.

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
