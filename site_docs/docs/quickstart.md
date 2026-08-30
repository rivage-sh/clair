# Quickstart

This guide shows you how to build your first clair project and how to run it.

## 1. Set up an environment

Run `clair init`. The command asks for your Snowflake connection details, then writes `~/.clair/environments.yml`:

```
$ clair init

Project directory [.]: ./my_project

  hint: select concat(current_organization_name(), '-', current_account_name()) as account;
Snowflake account (e.g. myorg-myaccount): myorg-myaccount

  hint: select current_user() as user;
Snowflake user: alice@example.com

Authentication method:
  1. Private key
  2. Password
  3. SSO (externalbrowser)
Enter choice [1]: 3

  hint: select current_warehouse() as warehouse;
Warehouse: my_warehouse

Role (leave blank to use user default): analyst

  hint: select current_region() as region;
Region (e.g. us-east-1): us-east-1

  hint: select current_account() as account_locator;
Account locator (e.g. abc12345): abc12345

Trouves that run at one time (threads) [4]: 4

Give an example Snowflake table that contains source data (for example source.orders.raw) [source]: source.products.catalog

  created  /path/to/my_project/source/products/catalog.py
  created  /path/to/my_project/.gitignore
  created  ~/.clair/environments.yml

✓ Project ready.

Next steps:
  1. cd /path/to/my_project
  2. clair compile
  3. clair run
```

## 2. Create a project

Each directory level maps to one part of the Snowflake name: `database / schema / table`.

```
my_project/
├── source/
│   └── products/
│       ├── catalog.py             # source.products.catalog
│       └── reviews.py             # source.products.reviews
├── refined/
│   └── products/
│       ├── catalog.py             # refined.products.catalog
│       └── reviews.py             # refined.products.reviews
└── derived/
    └── products/
        └── top_reviewed.py        # derived.products.top_reviewed
```

## 3. Write your Trouves

**`source/products/catalog.py`** — declares a Snowflake table that already exists:

```python
from clair import Column, ColumnType, Trouve, TrouveType

trouve = Trouve(
    type=TrouveType.SOURCE,
    columns=[
        Column(name="product_id",   type=ColumnType.STRING),
        Column(name="name",         type=ColumnType.STRING),
        Column(name="category",     type=ColumnType.STRING),
        Column(name="price",        type=ColumnType.FLOAT),
    ],
)
```

**`refined/products/catalog.py`** — cleans and standardises the source:

```python
from clair import Trouve, TrouveType
from source.products.catalog import trouve as source_catalog

trouve = Trouve(
    type=TrouveType.TABLE,
    sql=f"""
        SELECT
            product_id,
            initcap(name)     AS name,
            lower(category)   AS category,
            price
        FROM {source_catalog}
        WHERE product_id IS NOT NULL
    """,
)
```

**`refined/products/reviews.py`** — cleans the reviews source:

```python
from clair import Trouve, TrouveType
from source.products.reviews import trouve as source_reviews

trouve = Trouve(
    type=TrouveType.TABLE,
    sql=f"""
        SELECT
            review_id,
            product_id,
            rating,
            review_date
        FROM {source_reviews}
        WHERE rating BETWEEN 1 AND 5
    """,
)
```

**`derived/products/top_reviewed.py`** — joins and aggregates the refined layer:

```python
from clair import Trouve, TrouveType
from refined.products.catalog import trouve as catalog
from refined.products.reviews import trouve as reviews

trouve = Trouve(
    type=TrouveType.TABLE,
    sql=f"""
        SELECT
            c.product_id,
            c.name,
            c.category,
            avg(r.rating)  AS avg_rating,
            count(*)       AS review_count
        FROM {catalog} c
        JOIN {reviews} r ON c.product_id = r.product_id
        GROUP BY 1, 2, 3
        HAVING avg_rating >= 4
    """,
)
```

## 4. Compile and run

```bash
cd my_project

# Read the generated SQL. This does not connect to Snowflake.
clair compile

# Run against Snowflake
clair run --env=dev
```

## Next steps

- [Pandas-native transformations](topics/pandas-native.md) — write pipeline steps as Python functions
- [Incrementality](topics/incrementality.md) — APPEND and UPSERT strategies for large tables
- [Data quality tests](topics/data-quality-tests.md) — all four test types
- [Selectors](topics/selectors.md) — run only a subset of Trouves
- [CLI reference](cli/overview.md) — all commands and flags
