from clair import Column, ColumnType, Trouve, TrouveType

trouve = Trouve(
    type=TrouveType.SOURCE,
    docs="Raw products catalog table.",
    columns=[
        Column(name="product_id", type=ColumnType.STRING),
        Column(name="seller_id", type=ColumnType.STRING),
        Column(name="title", type=ColumnType.STRING),
        Column(name="category", type=ColumnType.STRING),
        Column(name="subcategory", type=ColumnType.STRING),
        Column(name="price", type=ColumnType.FLOAT),
        Column(name="attributes", type=ColumnType.VARIANT),
        Column(name="created_at", type=ColumnType.TIMESTAMP_NTZ),
    ],
)
