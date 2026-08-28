import pandas as pd

from clair import Column, ColumnType, SeedTrouve

# A seed holds its rows in this file. Set the dtype of each column, because the
# Snowflake type of a seed column comes from the dtype of that column.
event_type_labels = pd.DataFrame(
    {
        "event_type": ["page_view", "add_to_cart", "purchase"],
        "label": ["Page view", "Add to cart", "Purchase"],
        "is_conversion": [False, False, True],
    }
)
event_type_labels["event_type"] = event_type_labels["event_type"].astype("string")
event_type_labels["label"] = event_type_labels["label"].astype("string")

trouve = SeedTrouve(
    dataframe=event_type_labels,
    docs="The label of each event type. A person maintains this table by hand.",
    columns=[
        Column(name="event_type", type=ColumnType.STRING),
        Column(name="label", type=ColumnType.STRING),
        Column(name="is_conversion", type=ColumnType.BOOLEAN),
    ],
)
