# Quickstart

This page takes you from an empty directory to a first run. You make two source tables
in Snowflake, write three Trouves that read them, and run the project.

You need [clair](installation.md), a Snowflake account, and a role that can make a
database.

## 1. Set up an environment

Run `clair init`. It asks for the connection details of Snowflake, and writes two files:
`~/.clair/environments.yml` holds the connection, and `__routing__.py` marks the root of
the project.

```
$ clair init

Project directory [.]: ./my_project

Environment name [dev]: dev

  hint: select concat(current_organization_name(), '-', current_account_name()) as account;
Snowflake account (e.g. myorg-myaccount): myorg-myaccount

  hint: select current_user() as user;
Snowflake user: alice@example.com

Authentication method:
  1. Private key
  2. Password
  3. SSO (externalbrowser)
Enter choice (1, 2, 3) [1]: 3

  hint: select current_warehouse() as warehouse;
Warehouse: my_warehouse

Role (leave empty to use the default role of the user): analyst

  hint: select current_region() as region;
Region (e.g. us-east-1): us-east-1

  hint: select current_account() as account_locator;
Account locator (e.g. abc12345): abc12345

Trouves that run at one time (threads) [4]: 4

Give an example Snowflake table that contains source data (for example source.orders.raw) [source]: source.products.catalog

  created  /path/to/my_project/source/products/catalog.py
  created  /path/to/my_project/__routing__.py

  created  /path/to/my_project/.gitignore

✓ Project ready.

Next steps:
  1. cd /path/to/my_project
  2. clair compile --env dev
  3. clair run --env dev
```

Each command after this one takes `--env dev`.

!!! warning "The `dev` entry writes to the production names"
    The entry that `clair init` writes changes no address. Thus `clair run --env dev`
    writes `refined.products.catalog`, which is also the production name. That is correct
    for this page. Before you share the project, give `dev` its own database or schema:
    see [Routing](topics/routing.md).

## 2. Make the source tables

Run this SQL in Snowflake. It makes the two tables that the project reads:

```sql
CREATE DATABASE IF NOT EXISTS source;
CREATE SCHEMA IF NOT EXISTS source.products;

CREATE OR REPLACE TABLE source.products.catalog AS
SELECT * FROM VALUES
    ('p1', 'blue mug',    'Kitchen', 12.0),
    ('p2', 'green chair', 'Garden',  80.0),
    (NULL, 'lost item',   'Misc',     1.0)
AS t (product_id, name, category, price);

CREATE OR REPLACE TABLE source.products.reviews AS
SELECT * FROM VALUES
    ('r1', 'p1', 5, '2026-01-02'::date),
    ('r2', 'p1', 4, '2026-01-05'::date),
    ('r3', 'p2', 2, '2026-01-07'::date),
    ('r4', 'p2', 9, '2026-01-09'::date)
AS t (review_id, product_id, rating, review_date);
```

## 3. Write the Trouves

Each directory level gives one part of the Snowflake name: `database / schema / table`.

```
my_project/
├── __routing__.py
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

A `Source` declares a table that already exists. clair reads it, and never writes it.
Replace the file that `clair init` wrote:

```python
# source/products/catalog.py
from clair import Column, ColumnType, Source

trouve = Source(
    columns=[
        Column(name="product_id",   type=ColumnType.STRING),
        Column(name="name",         type=ColumnType.STRING),
        Column(name="category",     type=ColumnType.STRING),
        Column(name="price",        type=ColumnType.FLOAT),
    ],
)
```

```python
# source/products/reviews.py
from clair import Column, ColumnType, Source

trouve = Source(
    columns=[
        Column(name="review_id",    type=ColumnType.STRING),
        Column(name="product_id",   type=ColumnType.STRING),
        Column(name="rating",       type=ColumnType.INTEGER),
        Column(name="review_date",  type=ColumnType.DATE),
    ],
)
```

A `Table` makes a table from its SQL. Import the upstream Trouve, and put it in the
f-string. clair replaces it with the Snowflake name, and adds the edge to the DAG.

```python
# refined/products/catalog.py
from clair import Table, TestNotNull, TestUnique
from source.products.catalog import trouve as source_catalog

trouve = Table(
    sql=f"""
        SELECT
            product_id,
            initcap(name)     AS name,
            lower(category)   AS category,
            price
        FROM {source_catalog}
        WHERE product_id IS NOT NULL
    """,
    tests=[TestUnique(column="product_id"), TestNotNull(column="product_id")],
)
```

```python
# refined/products/reviews.py
from clair import Table
from source.products.reviews import trouve as source_reviews

trouve = Table(
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

```python
# derived/products/top_reviewed.py
from clair import Table
from refined.products.catalog import trouve as catalog
from refined.products.reviews import trouve as reviews

trouve = Table(
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

## 4. Look at the DAG and the SQL

These commands need no Snowflake connection:

```bash
cd my_project
clair dag
clair compile --env dev
```

`clair dag` prints the tree. `clair compile` writes each statement of the run to
`_clairtifacts/<run_id>/`, one `.sql` file for each Trouve. Open
`_clairtifacts/<run_id>/refined/products/catalog.sql`: the build writes to a staging
table, the tests read that table, and the promotion replaces production.

## 5. Run

```bash
clair run --env dev
```

clair makes the databases `refined` and `derived`, and builds the three Trouves in order.
The tests of `refined.products.catalog` run before its promotion. Query the result:

```sql
SELECT * FROM derived.products.top_reviewed;
```

The row for `p1` has an `avg_rating` of 4.5. The review with a rating of 9 does not pass
the filter of `refined.products.reviews`, thus `p2` has an average of 2 and no row.

## Next steps

- [Trouve](topics/trouve.md) — each kind of Trouve, and its fields
- [Data quality tests](topics/data-quality-tests.md) — each test type, and the severity
- [Incrementality](topics/incrementality.md) — APPEND, UPSERT and DELETE_INSERT for large tables
- [Routing](topics/routing.md) — give each environment its own database or schema
- [Selectors](topics/selectors.md) — run a part of the project
