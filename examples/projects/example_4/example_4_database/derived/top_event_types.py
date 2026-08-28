from example_4_database.derived.daily_event_counts import trouve as example_4_database_derived_daily_event_counts
from example_4_database.reference.event_type_labels import trouve as example_4_database_reference_event_type_labels

from clair import Column, ColumnType, Trouve

trouve = Trouve(
    docs="The 10 event types with the highest total count for all time.",
    sql=f"""
        select
            counts.event_type,
            labels.label,
            sum(counts.event_count) as total_event_count
        from {example_4_database_derived_daily_event_counts} as counts
        left join {example_4_database_reference_event_type_labels} as labels
            on labels.event_type = counts.event_type
        group by counts.event_type, labels.label
        order by total_event_count desc
        limit 10
    """,
    columns=[
        Column(name="event_type", type=ColumnType.STRING),
        Column(name="label", type=ColumnType.STRING),
        Column(name="total_event_count", type=ColumnType.NUMBER),
    ],
)
