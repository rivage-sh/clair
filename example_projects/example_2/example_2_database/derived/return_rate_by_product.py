from clair import Column, ColumnType, Trouve, TrouveType
from example_2_database.refined.order_items import trouve as example_2_database_refined_order_items
from example_2_database.refined.returns import trouve as example_2_database_refined_returns

trouve = Trouve(
    type=TrouveType.TABLE,
    docs="Per-product return rates based on order items.",
    sql=f"""
        select
            oi.product_id,
            count(distinct oi.order_item_id)                                as total_order_items,
            count(distinct r.return_id)                                     as total_returns,
            div0(count(distinct r.return_id), count(distinct oi.order_item_id)) as return_rate
        from {example_2_database_refined_order_items} oi
        left join {example_2_database_refined_returns} r
            on oi.order_item_id = r.order_item_id
        group by 1
    """,
    columns=[
        Column(name="product_id", type=ColumnType.STRING),
        Column(name="total_order_items", type=ColumnType.NUMBER),
        Column(name="total_returns", type=ColumnType.NUMBER),
        Column(name="return_rate", type=ColumnType.FLOAT),
    ],
)
