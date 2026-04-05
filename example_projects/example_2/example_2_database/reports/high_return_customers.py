from clair import Column, ColumnType, Trouve, TrouveType
from example_2_database.refined.returns import trouve as example_2_database_refined_returns
from example_2_database.refined.order_items import trouve as example_2_database_refined_order_items
from example_2_database.refined.users import trouve as example_2_database_refined_users

trouve = Trouve(
    type=TrouveType.TABLE,
    docs="Customers with return rates above 20 percent.",
    sql=f"""
        select
            u.user_id,
            u.email,
            u.full_name,
            count(distinct r.return_id)                                                 as total_returns,
            count(distinct oi.order_item_id)                                            as total_order_items,
            div0(count(distinct r.return_id), count(distinct oi.order_item_id))         as return_rate
        from {example_2_database_refined_returns} r
        join {example_2_database_refined_order_items} oi
            on r.order_item_id = oi.order_item_id
        join {example_2_database_refined_users} u
            on r.user_id = u.user_id
        group by 1, 2, 3
        having div0(count(distinct r.return_id), count(distinct oi.order_item_id)) > 0.2
        order by return_rate desc
    """,
    columns=[
        Column(name="user_id", type=ColumnType.STRING),
        Column(name="email", type=ColumnType.STRING),
        Column(name="full_name", type=ColumnType.STRING),
        Column(name="total_returns", type=ColumnType.NUMBER),
        Column(name="total_order_items", type=ColumnType.NUMBER),
        Column(name="return_rate", type=ColumnType.FLOAT),
    ],
)
