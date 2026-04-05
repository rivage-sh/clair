from clair import Trouve, TrouveType
from db.schema.b import trouve as b

trouve = Trouve(
    type=TrouveType.TABLE,
    sql=f"select * from {b}",
)
