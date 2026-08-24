from clair_ci.refined.events import trouve as clair_ci_refined_events
from clair_ci.refined.orders import trouve as clair_ci_refined_orders

from clair import Column, ColumnType, Trouve, TrouveType

trouve = Trouve(
    type=TrouveType.VIEW,
    docs="""
        One row for each user, with the event count and the order total. This Trouve is a
        VIEW, and it reads two schemas. The integration test uses it to prove that clair
        routes a VIEW and a cross-schema reference.
    """,
    sql=f"""
        select
            e.user_id,
            count(distinct e.event_id)      as event_count,
            coalesce(sum(o.amount), 0)      as order_amount
        from {clair_ci_refined_events} e
        left join {clair_ci_refined_orders} o
            on e.user_id = o.user_id
        group by 1
    """,
    columns=[
        Column(name="user_id", type=ColumnType.STRING),
        Column(name="event_count", type=ColumnType.NUMBER),
        Column(name="order_amount", type=ColumnType.FLOAT),
    ],
)
