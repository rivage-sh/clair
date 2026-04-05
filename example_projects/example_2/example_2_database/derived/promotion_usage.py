from clair import Column, ColumnType, Trouve, TrouveType
from example_2_database.refined.orders import trouve as example_2_database_refined_orders
from example_2_database.refined.promotions import trouve as example_2_database_refined_promotions

trouve = Trouve(
    type=TrouveType.TABLE,
    docs="Promotion usage statistics from orders with applied promos.",
    sql=f"""
        select
            p.promotion_id,
            p.code,
            p.discount_type,
            p.discount_value,
            count(*)                as times_used,
            sum(o.total_amount)     as gross_revenue_with_promo,
            avg(o.total_amount)     as avg_order_value_with_promo
        from {example_2_database_refined_orders} o
        join {example_2_database_refined_promotions} p
            on o.promotion_id = p.promotion_id
        where o.promotion_id is not null
        group by 1, 2, 3, 4
    """,
    columns=[
        Column(name="promotion_id", type=ColumnType.STRING),
        Column(name="code", type=ColumnType.STRING),
        Column(name="discount_type", type=ColumnType.STRING),
        Column(name="discount_value", type=ColumnType.FLOAT),
        Column(name="times_used", type=ColumnType.NUMBER),
        Column(name="gross_revenue_with_promo", type=ColumnType.FLOAT),
        Column(name="avg_order_value_with_promo", type=ColumnType.FLOAT),
    ],
)
