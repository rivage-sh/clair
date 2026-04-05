from clair import Column, ColumnType, Trouve, TrouveType
from example_2_database.source.users import trouve as example_2_database_source_users

trouve = Trouve(
    type=TrouveType.TABLE,
    docs="Refined users with flattened address and full name.",
    sql=f"""
        select
            user_id,
            email,
            first_name,
            last_name,
            first_name || ' ' || last_name      as full_name,
            created_at,
            address:street::string               as street,
            address:city::string                 as city,
            address:state::string                as state,
            address:country::string              as country,
            address:zip::string                  as zip_code,
            is_prime_member
        from {example_2_database_source_users}
    """,
    columns=[
        Column(name="user_id", type=ColumnType.STRING),
        Column(name="email", type=ColumnType.STRING),
        Column(name="first_name", type=ColumnType.STRING),
        Column(name="last_name", type=ColumnType.STRING),
        Column(name="full_name", type=ColumnType.STRING),
        Column(name="created_at", type=ColumnType.TIMESTAMP_NTZ),
        Column(name="street", type=ColumnType.STRING, nullable=True),
        Column(name="city", type=ColumnType.STRING, nullable=True),
        Column(name="state", type=ColumnType.STRING, nullable=True),
        Column(name="country", type=ColumnType.STRING, nullable=True),
        Column(name="zip_code", type=ColumnType.STRING, nullable=True),
        Column(name="is_prime_member", type=ColumnType.BOOLEAN),
    ],
)
