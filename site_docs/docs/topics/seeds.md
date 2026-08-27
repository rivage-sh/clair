# Seeds

A `SeedTrouve` is a table that holds its rows in the Python file. Use it for a small table
that a person maintains by hand: country codes, tax rates, or a map from an id to a label.

A seed reads no other Trouve, thus it is always a root of the DAG. clair builds it in the
same run as every other Trouve, in topological order, before each Trouve that reads it.
**There is no `clair seed` command.** A seed is an ordinary table, and the data comes from
the file in place of a `SELECT`.

## Basic example

```python
# example_4_database/reference/event_type_labels.py
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
```

A SQL Trouve reads a seed with the usual f-string reference:

```python
# example_4_database/derived/top_event_types.py
from example_4_database.derived.daily_event_counts import trouve as daily_event_counts
from example_4_database.reference.event_type_labels import trouve as event_type_labels

from clair import Trouve

trouve = Trouve(
    sql=f"""
        select
            counts.event_type,
            labels.label,
            sum(counts.event_count) as total_event_count
        from {daily_event_counts} as counts
        left join {event_type_labels} as labels
            on labels.event_type = counts.event_type
        group by counts.event_type, labels.label
    """,
)
```

`examples/projects/example_4/` holds the complete project.

## The dtype gives the Snowflake type

clair writes the DataFrame with the pandas write path of the Snowflake connector. That path
writes the rows as a Parquet file, puts the file on a temporary stage, and lets Snowflake
infer the schema of the file. Therefore the Snowflake type of each column comes from the
dtype of that column, and not from `columns`. `columns` stays documentation, the same as
for every other Trouve.

A text dtype gives a text column, an integer dtype gives a number column, a float dtype
gives a floating point column, a boolean dtype gives BOOLEAN, and a datetime dtype gives a
timestamp column.

To control a type, set the dtype in Python:

```python
frame["region_id"] = frame["region_id"].astype("Int64")
frame["valid_from"] = pd.to_datetime(frame["valid_from"])
```

!!! warning "Give an integer column with a null the `Int64` dtype"
    pandas turns `[1, 2, None]` into the `float64` dtype, and the ids reach Snowflake as
    `1.0` and `2.0`. The nullable `Int64` dtype keeps them as integers.

pandas has no decimal dtype, thus a seed cannot make a `NUMBER(18,2)` column. For an exact
decimal, cast in a Trouve that reads the seed.

## The shape of the data

`SeedTrouve` takes one DataFrame, and the pandas constructor accepts each usual shape:

```python
# A dict of columns.
pd.DataFrame({"code": ["US", "FR"], "rate": [0.0, 0.20]})

# A list of rows, with the column names.
pd.DataFrame([["US", 0.0], ["FR", 0.20]], columns=["code", "rate"])

# A list of dicts.
pd.DataFrame([{"code": "US", "rate": 0.0}, {"code": "FR", "rate": 0.20}])
```

The file is ordinary Python, thus you can compute the rows before you build the Trouve.

## How large a seed can be

Snowflake is not the limit — clair writes the rows as a file load, and not as a SQL
statement. The limit is the Python file. clair imports each Trouve file at discovery,
therefore a file with many thousands of rows makes every clair command slower, and the
diff of that file is hard to read.

Keep a seed to a few hundred rows. For a large table, make a `SOURCE` Trouve and load the
data with a tool that loads data.

## Migration from a dbt seed

clair reads no CSV file. A dbt seed is a CSV, thus you convert each one time. This script
writes one seed file for each CSV:

```python
from pathlib import Path

import pandas as pd

TEMPLATE = '''import pandas as pd

from clair import SeedTrouve

rows = {rows}

frame = pd.DataFrame(rows)

trouve = SeedTrouve(dataframe=frame)
'''

destination = Path("mydb/reference")
destination.mkdir(parents=True, exist_ok=True)

for csv_path in Path("seeds").glob("*.csv"):
    frame = pd.read_csv(csv_path, dtype="string")
    rows = frame.to_dict(orient="records")
    seed_path = destination / f"{csv_path.stem}.py"
    seed_path.write_text(TEMPLATE.format(rows=rows))
```

`dtype="string"` reads each column as text, thus a leading zero in a zip code or an id
stays. Then set the dtype of each column that is not text. The dbt `column_types` config
exists to repair the same problem, because a CSV holds text only. In clair the dtype does
that work, and it lives in the Python file.

## Constraints

- A seed is always `TrouveType.TABLE`. A VIEW or a SOURCE raises `ValueError`.
- A seed does not support the incremental run modes. clair replaces the table each run.
- Each column name of the DataFrame must be a string, and the names must be unique.
- The DataFrame needs one column minimum. A seed with no row is valid.

## What stays the same

The DAG, the lineage, the selectors, the [staging](staging.md) step, the
[data quality tests](data-quality-tests.md), [routing](routing.md), and `clair docs` all
apply to a seed, the same as to a SQL Trouve.

`clair compile` writes an artifact file for a seed. The file holds the dtypes and the rows,
in place of the SQL:

```
# clair compiled: omer.reference.event_type_labels
# execution_type: pandas

# 3 row(s), 3 column(s)
# dtypes:
#   event_type: string
#   label: string
#   is_conversion: bool

    event_type        label  is_conversion
0    page_view    Page view          False
1  add_to_cart  Add to cart          False
2     purchase     Purchase           True
```

For the field tables, read the [Trouve API](../reference/trouve-api.md).
