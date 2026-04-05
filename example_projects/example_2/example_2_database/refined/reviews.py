from clair import Column, ColumnType, Trouve, TrouveType
from example_2_database.source.reviews import trouve as example_2_database_source_reviews

trouve = Trouve(
    type=TrouveType.TABLE,
    docs="Refined reviews with sentiment classification.",
    sql=f"""
        select
            review_id,
            product_id,
            user_id,
            order_id,
            rating,
            title,
            body,
            created_at,
            case
                when rating >= 4 then 'positive'
                when rating = 3  then 'neutral'
                else 'negative'
            end as sentiment
        from {example_2_database_source_reviews}
    """,
    columns=[
        Column(name="review_id", type=ColumnType.STRING),
        Column(name="product_id", type=ColumnType.STRING),
        Column(name="user_id", type=ColumnType.STRING),
        Column(name="order_id", type=ColumnType.STRING),
        Column(name="rating", type=ColumnType.NUMBER),
        Column(name="title", type=ColumnType.STRING),
        Column(name="body", type=ColumnType.STRING),
        Column(name="created_at", type=ColumnType.TIMESTAMP_NTZ),
        Column(name="sentiment", type=ColumnType.STRING),
    ],
)
