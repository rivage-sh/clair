from clair import Column, ColumnType, Trouve, TrouveType
from example_2_database.source.sellers import trouve as example_2_database_source_sellers

trouve = Trouve(
    type=TrouveType.TABLE,
    docs="Refined sellers with flattened contact info.",
    sql=f"""
        select
            seller_id,
            name,
            email,
            contact_info:phone::string           as phone,
            contact_info:country::string         as country,
            contact_info:website::string         as website,
            joined_at,
            is_verified
        from {example_2_database_source_sellers}
    """,
    columns=[
        Column(name="seller_id", type=ColumnType.STRING),
        Column(name="name", type=ColumnType.STRING),
        Column(name="email", type=ColumnType.STRING),
        Column(name="phone", type=ColumnType.STRING, nullable=True),
        Column(name="country", type=ColumnType.STRING, nullable=True),
        Column(name="website", type=ColumnType.STRING, nullable=True),
        Column(name="joined_at", type=ColumnType.TIMESTAMP_NTZ),
        Column(name="is_verified", type=ColumnType.BOOLEAN),
    ],
)
