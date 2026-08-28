import pandas as pd

from clair import Column, ColumnType, SeedTrouve

# A seed holds its rows in this file. Set the dtype of each column, because the
# Snowflake type of a seed column comes from the dtype of that column.
country_regions = pd.DataFrame(
    {
        "country": ["US", "CA", "MX", "GB", "FR", "DE", "ES", "JP", "AU", "BR"],
        "region": [
            "North America",
            "North America",
            "North America",
            "Europe",
            "Europe",
            "Europe",
            "Europe",
            "Asia Pacific",
            "Asia Pacific",
            "South America",
        ],
        "is_domestic": [True, False, False, False, False, False, False, False, False, False],
    }
)
country_regions["country"] = country_regions["country"].astype("string")
country_regions["region"] = country_regions["region"].astype("string")

trouve = SeedTrouve(
    dataframe=country_regions,
    docs="The sales region of each country. A person maintains this table by hand.",
    columns=[
        Column(name="country", type=ColumnType.STRING),
        Column(name="region", type=ColumnType.STRING),
        Column(name="is_domestic", type=ColumnType.BOOLEAN),
    ],
)
