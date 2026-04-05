from clair import Column, ColumnType, Trouve, TrouveType
from example_2_database.derived.category_sales_summary import trouve as example_2_database_derived_category_sales_summary

trouve = Trouve(
    type=TrouveType.TABLE,
    docs="Categories ranked by month-over-month revenue growth.",
    sql=f"""
        with this_month as (
            select
                category,
                net_revenue
            from {example_2_database_derived_category_sales_summary}
            where order_month = date_trunc('month', dateadd('month', -1, current_date()))::date
        ),
        last_month as (
            select
                category,
                net_revenue
            from {example_2_database_derived_category_sales_summary}
            where order_month = date_trunc('month', dateadd('month', -2, current_date()))::date
        )
        select
            coalesce(tm.category, lm.category)                              as category,
            coalesce(lm.net_revenue, 0)                                     as last_month_revenue,
            coalesce(tm.net_revenue, 0)                                     as this_month_revenue,
            div0(
                coalesce(tm.net_revenue, 0) - coalesce(lm.net_revenue, 0),
                coalesce(lm.net_revenue, 0)
            )                                                               as mom_growth_rate
        from this_month tm
        full outer join last_month lm
            on tm.category = lm.category
        order by mom_growth_rate desc
    """,
    columns=[
        Column(name="category", type=ColumnType.STRING),
        Column(name="last_month_revenue", type=ColumnType.FLOAT),
        Column(name="this_month_revenue", type=ColumnType.FLOAT),
        Column(name="mom_growth_rate", type=ColumnType.FLOAT),
    ],
)
