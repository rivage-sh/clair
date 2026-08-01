from db.schema.a import trouve as a

from clair import Trouve, TrouveType

trouve = Trouve(
    type=TrouveType.TABLE,
    sql=f"select * from {a}",
)
