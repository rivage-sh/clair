from clair import Column, ColumnType, Trouve, TrouveType
from example_2_database.derived.product_review_summary import trouve as example_2_database_derived_product_review_summary
from example_2_database.refined.products import trouve as example_2_database_refined_products

trouve = Trouve(
    type=TrouveType.TABLE,
    docs="Highly rated products with at least 10 reviews and avg rating >= 4.5.",
    sql=f"""
        select
            p.product_id,
            p.title,
            p.category,
            p.seller_id,
            pr.review_count,
            pr.avg_rating,
            pr.positive_reviews,
            pr.negative_reviews,
            pr.neutral_reviews
        from {example_2_database_derived_product_review_summary} pr
        join {example_2_database_refined_products} p
            on pr.product_id = p.product_id
        where pr.avg_rating >= 4.5
          and pr.review_count >= 10
        order by pr.avg_rating desc, pr.review_count desc
    """,
    columns=[
        Column(name="product_id", type=ColumnType.STRING),
        Column(name="title", type=ColumnType.STRING),
        Column(name="category", type=ColumnType.STRING),
        Column(name="seller_id", type=ColumnType.STRING),
        Column(name="review_count", type=ColumnType.NUMBER),
        Column(name="avg_rating", type=ColumnType.FLOAT),
        Column(name="positive_reviews", type=ColumnType.NUMBER),
        Column(name="negative_reviews", type=ColumnType.NUMBER),
        Column(name="neutral_reviews", type=ColumnType.NUMBER),
    ],
)
