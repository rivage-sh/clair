from db.schema.b import trouve as b

from clair import Table

trouve = Table(
    sql=f"select * from {b}",
)
