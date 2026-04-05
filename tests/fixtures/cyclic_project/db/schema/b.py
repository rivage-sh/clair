from clair import Trouve, TrouveType
from db.schema.a import trouve as a

trouve = Trouve(
    type=TrouveType.TABLE,
    sql=f"select * from {a}",
)
