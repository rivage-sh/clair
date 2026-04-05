from clair import Column, ColumnType, Trouve, TrouveType
from example_2_database.refined.inventory import trouve as example_2_database_refined_inventory
from example_2_database.refined.products import trouve as example_2_database_refined_products
from example_2_database.derived.product_sales_summary import trouve as example_2_database_derived_product_sales_summary

trouve = Trouve(
    type=TrouveType.TABLE,
    docs="Products with fewer than 30 days of stock remaining.",
    sql=f"""
        with daily_sales_rate as (
            select
                product_id,
                sum(units_sold) / 30.0 as avg_daily_sales
            from {example_2_database_derived_product_sales_summary}
            group by 1
        )
        select
            i.product_id,
            p.title                                             as product_title,
            p.category,
            i.warehouse_id,
            i.quantity_on_hand,
            dsr.avg_daily_sales,
            div0(i.quantity_on_hand, dsr.avg_daily_sales)       as days_of_stock_remaining
        from {example_2_database_refined_inventory} i
        join {example_2_database_refined_products} p
            on i.product_id = p.product_id
        join daily_sales_rate dsr
            on i.product_id = dsr.product_id
        where div0(i.quantity_on_hand, dsr.avg_daily_sales) < 30
        order by days_of_stock_remaining asc
    """,
    columns=[
        Column(name="product_id", type=ColumnType.STRING),
        Column(name="product_title", type=ColumnType.STRING),
        Column(name="category", type=ColumnType.STRING),
        Column(name="warehouse_id", type=ColumnType.STRING),
        Column(name="quantity_on_hand", type=ColumnType.NUMBER),
        Column(name="avg_daily_sales", type=ColumnType.FLOAT),
        Column(name="days_of_stock_remaining", type=ColumnType.FLOAT),
    ],
)
