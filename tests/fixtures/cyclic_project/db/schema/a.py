from db.schema.b import trouve as b

from clair import Trouve, TrouveType

trouve = Trouve(
    type=TrouveType.TABLE,
    sql=f"select * from {b}",
)
