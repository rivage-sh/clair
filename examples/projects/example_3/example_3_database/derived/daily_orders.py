from example_3_database.refined.orders import trouve as refined_orders

import clair
from clair import (
    Column,
    ColumnType,
    DeleteInsert,
    Incremental,
    RunMode,
    TestUnique,
    Trouve,
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

trouve = Trouve(
    type=TrouveType.TABLE,
    docs="""
        The orders and the revenue of each day. In each incremental run, clair calculates
        the last 3 days again and replaces them. Thus a late order changes the total of its
        day, and the days before the window stay.
    """,
    sql=sql,
    run_config=Incremental(strategy=DeleteInsert(primary_key_columns=["created_date"])),
    columns=[
        Column(name="created_date", type=ColumnType.DATE),
        Column(name="order_count", type=ColumnType.INTEGER),
        Column(name="revenue", type=ColumnType.FLOAT),
    ],
    tests=[TestUnique(column="created_date")],
)
