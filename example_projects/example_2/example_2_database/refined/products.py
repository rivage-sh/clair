from clair import Column, ColumnType, Trouve, TrouveType
from example_2_database.source.products import trouve as example_2_database_source_products

trouve = Trouve(
    type=TrouveType.TABLE,
    docs="Refined products with flattened attributes.",
    sql=f"""
        select
            product_id,
            seller_id,
            title,
            category,
            subcategory,
            price,
            attributes:brand::string             as brand,
            attributes:weight_kg::float          as weight_kg,
            attributes:color::string             as color,
            attributes:material::string          as material,
            created_at
        from {example_2_database_source_products}
    """,
    columns=[
        Column(name="product_id", type=ColumnType.STRING),
        Column(name="seller_id", type=ColumnType.STRING),
        Column(name="title", type=ColumnType.STRING),
        Column(name="category", type=ColumnType.STRING),
        Column(name="subcategory", type=ColumnType.STRING),
        Column(name="price", type=ColumnType.FLOAT),
        Column(name="brand", type=ColumnType.STRING, nullable=True),
        Column(name="weight_kg", type=ColumnType.FLOAT, nullable=True),
        Column(name="color", type=ColumnType.STRING, nullable=True),
        Column(name="material", type=ColumnType.STRING, nullable=True),
        Column(name="created_at", type=ColumnType.TIMESTAMP_NTZ),
    ],
)
