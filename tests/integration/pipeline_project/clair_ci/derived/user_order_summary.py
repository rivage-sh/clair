from clair_ci.refined.orders import trouve as clair_ci_refined_orders

from clair import (
    Column,
    ColumnType,
    IncrementalMode,
    RunConfig,
    RunMode,
    TestUnique,
    Trouve,
    TrouveType,
)

trouve = Trouve(
    type=TrouveType.TABLE,
    docs="""
        Order statistics for each user. clair merges the rows on user_id. An incremental
        run gives the same row count as a full refresh, thus the integration test proves
        that MERGE updates the rows and does not duplicate them.
    """,
    sql=f"""
        select
            user_id,
            count(*)                        as order_count,
            sum(amount)                     as total_amount,
            max(created_at)                 as last_order_at
        from {clair_ci_refined_orders}
        group by user_id
    """,
    run_config=RunConfig(
        run_mode=RunMode.INCREMENTAL,
        incremental_mode=IncrementalMode.UPSERT,
        primary_key_columns=["user_id"],
    ),
    columns=[
        Column(name="user_id", type=ColumnType.STRING),
        Column(name="order_count", type=ColumnType.NUMBER),
        Column(name="total_amount", type=ColumnType.FLOAT),
        Column(name="last_order_at", type=ColumnType.TIMESTAMP_NTZ),
    ],
    tests=[
        TestUnique(column="user_id"),
    ],
)
