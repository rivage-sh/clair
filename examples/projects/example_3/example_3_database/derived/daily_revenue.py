from _replace_window_trouve import ReplaceWindowTrouve
from example_3_database.refined.orders import trouve as refined_orders

import clair
from clair import (
    Column,
    ColumnType,
    IncrementalMode,
    RunConfig,
    RunMode,
    TestUnique,
    TrouveType,
)

sql = f"""
    select
        created_date,
        count(*)    as order_count,
        sum(amount) as revenue
    from {refined_orders}
"""
if clair.run_mode == RunMode.INCREMENTAL:
    sql += """
        where created_date >= dateadd('day', -3, current_date())
    """
sql += """
    group by created_date
"""

trouve = ReplaceWindowTrouve(
    type=TrouveType.TABLE,
    docs="""
        The revenue of each day. In each incremental run, clair replaces the last 3 days,
        thus a late order changes the total of its day. ReplaceWindowTrouve overrides
        build_sql(), and its plan replaces the APPEND statement.
    """,
    sql=sql,
    window_column="created_date",
    run_config=RunConfig(
        run_mode=RunMode.INCREMENTAL,
        incremental_mode=IncrementalMode.APPEND,
    ),
    columns=[
        Column(name="created_date", type=ColumnType.DATE),
        Column(name="order_count", type=ColumnType.INTEGER),
        Column(name="revenue", type=ColumnType.FLOAT),
    ],
    tests=[TestUnique(column="created_date")],
)
