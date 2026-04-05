from clair import Column, ColumnType, Trouve, TrouveType
from example_2_database.source.order_items import trouve as example_2_database_source_order_items

trouve = Trouve(
    type=TrouveType.TABLE,
    docs="Refined order items with computed gross and net totals.",
    sql=f"""
        select
            order_item_id,
            order_id,
            product_id,
            quantity,
            unit_price,
            discount_amount,
            quantity * unit_price                                    as gross_total,
            quantity * unit_price - coalesce(discount_amount, 0)     as net_total
        from {example_2_database_source_order_items}
    """,
    columns=[
        Column(name="order_item_id", type=ColumnType.STRING),
        Column(name="order_id", type=ColumnType.STRING),
        Column(name="product_id", type=ColumnType.STRING),
        Column(name="quantity", type=ColumnType.NUMBER),
        Column(name="unit_price", type=ColumnType.FLOAT),
        Column(name="discount_amount", type=ColumnType.FLOAT, nullable=True),
        Column(name="gross_total", type=ColumnType.FLOAT),
        Column(name="net_total", type=ColumnType.FLOAT),
    ],
)
