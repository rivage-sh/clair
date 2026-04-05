from clair import Column, ColumnType, Trouve, TrouveType
from example_2_database.refined.reviews import trouve as example_2_database_refined_reviews

trouve = Trouve(
    type=TrouveType.TABLE,
    docs="Per-product review aggregates with sentiment breakdown.",
    sql=f"""
        select
            product_id,
            count(*)                                        as review_count,
            avg(rating)                                     as avg_rating,
            count_if(sentiment = 'positive')                as positive_reviews,
            count_if(sentiment = 'negative')                as negative_reviews,
            count_if(sentiment = 'neutral')                 as neutral_reviews
        from {example_2_database_refined_reviews}
        group by 1
    """,
    columns=[
        Column(name="product_id", type=ColumnType.STRING),
        Column(name="review_count", type=ColumnType.NUMBER),
        Column(name="avg_rating", type=ColumnType.FLOAT),
        Column(name="positive_reviews", type=ColumnType.NUMBER),
        Column(name="negative_reviews", type=ColumnType.NUMBER),
        Column(name="neutral_reviews", type=ColumnType.NUMBER),
    ],
)
