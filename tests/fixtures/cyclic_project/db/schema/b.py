from db.schema.a import trouve as a

from clair import Table

trouve = Table(
    sql=f"select * from {a}",
)
