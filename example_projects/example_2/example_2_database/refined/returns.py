from clair import Column, ColumnType, Trouve, TrouveType
from example_2_database.source.returns import trouve as example_2_database_source_returns

trouve = Trouve(
    type=TrouveType.TABLE,
    docs="Refined returns with reason category classification.",
    sql=f"""
        select
            return_id,
            order_item_id,
            user_id,
            reason,
            status,
            created_at,
            refund_amount,
            case
                when reason ilike '%defect%'
                  or reason ilike '%broken%'
                  or reason ilike '%damage%'          then 'defective'
                when reason ilike '%wrong%'
                  or reason ilike '%not as described%' then 'wrong_item'
                when reason ilike '%changed%'
                  or reason ilike '%no longer%'        then 'changed_mind'
                when reason ilike '%size%'
                  or reason ilike '%fit%'               then 'sizing'
                else 'other'
            end as reason_category
        from {example_2_database_source_returns}
    """,
    columns=[
        Column(name="return_id", type=ColumnType.STRING),
        Column(name="order_item_id", type=ColumnType.STRING),
        Column(name="user_id", type=ColumnType.STRING),
        Column(name="reason", type=ColumnType.STRING),
        Column(name="status", type=ColumnType.STRING),
        Column(name="created_at", type=ColumnType.TIMESTAMP_NTZ),
        Column(name="refund_amount", type=ColumnType.FLOAT),
        Column(name="reason_category", type=ColumnType.STRING),
    ],
)
