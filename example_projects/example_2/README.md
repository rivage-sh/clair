# Clair Example Project — E-Commerce

A complex Clair project with 50 Trouves modelling an Amazon-style e-commerce platform, intended for integration testing against a real Snowflake account.

## What it does

This project defines a four-layer analytics pipeline:

| Layer | Tables | Description |
|---|---|---|
| **source** (10) | users, products, orders, order_items, events, reviews, sellers, inventory, promotions, returns | Pre-existing operational tables |
| **refined** (10) | *(one per source)* | Flatten VARIANT columns, add convenience fields, cast types |
| **derived** (20) | daily/monthly order summaries, user stats, product sales, seller performance, funnel, returns, inventory alerts, … | Aggregations and joins across refined tables |
| **reports** (10) | top/bottom customers, top/bottom products, top sellers, churned users, high-return customers, best-reviewed products, trending categories, at-risk inventory | Business intelligence slices |

Lineage: `source.*` → `refined.*` → `derived.*` → `reports.*`

## Prerequisites

You need a Snowflake account with:

- A profile configured at `~/.clair/profiles.yml` (the `local` profile is used below)
- All ten source tables created and seeded (SQL below)

### Install clair

From the **project root** (`clair/`), run:

```bash
uv sync
source .venv/bin/activate
```

### Seed the source tables

Run the following SQL blocks in Snowflake in order (foreign keys flow downward).

```sql
create database if not exists example_2_database;
create schema if not exists example_2_database.source;
```

#### sellers

```sql
create table example_2_database.source.sellers as
select seller_id, name, email, parse_json(contact_info) as contact_info, joined_at, is_verified
from values
    ('sel_001', 'TechGadgets Inc',     'hello@techgadgets.io',   '{"phone":"+1-800-111-2222","country":"US","website":"techgadgets.io"}',    '2021-03-10 09:00:00'::timestamp_ntz, true),
    ('sel_002', 'HomeEssentials Co',   'support@homeessentials.com', '{"phone":"+1-800-333-4444","country":"US","website":"homeessentials.com"}', '2020-07-22 11:30:00'::timestamp_ntz, true),
    ('sel_003', 'SportsPro LLC',       'info@sportspro.com',     '{"phone":"+1-800-555-6666","country":"US","website":"sportspro.com"}',    '2022-01-05 08:00:00'::timestamp_ntz, false)
as t(seller_id, name, email, contact_info, joined_at, is_verified);
```

#### users

```sql
create table example_2_database.source.users as
select user_id, email, first_name, last_name, created_at, parse_json(address) as address, is_prime_member
from values
    ('usr_001', 'alice@example.com',  'Alice',  'Johnson', '2022-06-01 10:00:00'::timestamp_ntz, '{"street":"123 Maple St","city":"New York","state":"NY","country":"US","zip":"10001"}',    true),
    ('usr_002', 'bob@example.com',    'Bob',    'Smith',   '2023-01-14 15:22:00'::timestamp_ntz, '{"street":"456 Oak Ave","city":"Los Angeles","state":"CA","country":"US","zip":"90001"}',  false),
    ('usr_003', 'carol@example.com',  'Carol',  'White',   '2021-11-30 08:45:00'::timestamp_ntz, '{"street":"789 Pine Rd","city":"Austin","state":"TX","country":"US","zip":"73301"}',      true),
    ('usr_004', 'david@example.com',  'David',  'Brown',   '2023-05-18 12:00:00'::timestamp_ntz, '{"street":"321 Elm St","city":"Seattle","state":"WA","country":"US","zip":"98101"}',      false),
    ('usr_005', 'emma@example.com',   'Emma',   'Davis',   '2022-09-09 17:30:00'::timestamp_ntz, '{"street":"654 Cedar Blvd","city":"Chicago","state":"IL","country":"US","zip":"60601"}',  true)
as t(user_id, email, first_name, last_name, created_at, address, is_prime_member);
```

#### products

```sql
create table example_2_database.source.products as
select product_id, seller_id, title, category, subcategory, price, parse_json(attributes) as attributes, created_at
from values
    ('prod_001', 'sel_001', 'Wireless Headphones',  'Electronics', 'Audio',       79.99,  '{"brand":"SoundWave","weight_kg":0.25,"color":"Black","material":"Plastic"}',  '2021-06-01 00:00:00'::timestamp_ntz),
    ('prod_002', 'sel_001', 'USB-C Hub 7-in-1',     'Electronics', 'Accessories', 34.99,  '{"brand":"SoundWave","weight_kg":0.12,"color":"Silver","material":"Aluminum"}', '2021-09-15 00:00:00'::timestamp_ntz),
    ('prod_003', 'sel_003', 'Yoga Mat Extra Thick',  'Sports',      'Fitness',     29.99,  '{"brand":"FlexFit","weight_kg":1.20,"color":"Purple","material":"TPE"}',        '2022-02-20 00:00:00'::timestamp_ntz),
    ('prod_004', 'sel_002', '12-Cup Coffee Maker',   'Kitchen',     'Appliances',  89.99,  '{"brand":"BrewMaster","weight_kg":2.40,"color":"Stainless","material":"Steel"}','2020-11-01 00:00:00'::timestamp_ntz),
    ('prod_005', 'sel_003', 'Running Shoes Pro',     'Sports',      'Footwear',    119.99, '{"brand":"FlexFit","weight_kg":0.60,"color":"Blue","material":"Mesh"}',         '2022-05-10 00:00:00'::timestamp_ntz),
    ('prod_006', 'sel_002', 'LED Desk Lamp',         'Office',      'Lighting',    24.99,  '{"brand":"BrightHome","weight_kg":0.45,"color":"White","material":"ABS"}',      '2021-01-12 00:00:00'::timestamp_ntz)
as t(product_id, seller_id, title, category, subcategory, price, attributes, created_at);
```

#### promotions

```sql
create table example_2_database.source.promotions as
select promotion_id, code, discount_type, discount_value, parse_json(rules) as rules, starts_at, ends_at
from values
    ('promo_001', 'SAVE10',   'percentage', 10.0, '{"min_order_value":50.0,"eligible_categories":["Electronics","Kitchen"],"max_uses":1000}', '2024-01-01 00:00:00'::timestamp_ntz, '2024-12-31 23:59:59'::timestamp_ntz),
    ('promo_002', 'WELCOME5', 'flat',        5.0, '{"min_order_value":25.0,"eligible_categories":null,"max_uses":500}',                       '2024-01-01 00:00:00'::timestamp_ntz, '2024-06-30 23:59:59'::timestamp_ntz)
as t(promotion_id, code, discount_type, discount_value, rules, starts_at, ends_at);
```

#### orders

```sql
create table example_2_database.source.orders as
select order_id, user_id, status, created_at, shipped_at, delivered_at, promotion_id, total_amount
from values
    ('ord_001', 'usr_001', 'delivered',  '2024-01-15 10:00:00'::timestamp_ntz, '2024-01-16 14:00:00'::timestamp_ntz, '2024-01-18 11:00:00'::timestamp_ntz, null,       114.98),
    ('ord_002', 'usr_002', 'delivered',  '2024-01-16 15:30:00'::timestamp_ntz, '2024-01-17 10:00:00'::timestamp_ntz, '2024-01-20 09:00:00'::timestamp_ntz, null,        84.97),
    ('ord_003', 'usr_001', 'delivered',  '2024-01-22 09:15:00'::timestamp_ntz, '2024-01-23 12:00:00'::timestamp_ntz, '2024-01-25 14:00:00'::timestamp_ntz, 'promo_001', 80.99),
    ('ord_004', 'usr_003', 'delivered',  '2024-02-03 14:00:00'::timestamp_ntz, '2024-02-04 11:00:00'::timestamp_ntz, '2024-02-07 16:00:00'::timestamp_ntz, null,       149.98),
    ('ord_005', 'usr_004', 'delivered',  '2024-02-10 08:30:00'::timestamp_ntz, '2024-02-11 09:00:00'::timestamp_ntz, '2024-02-13 14:30:00'::timestamp_ntz, null,        79.99),
    ('ord_006', 'usr_001', 'delivered',  '2024-02-18 11:45:00'::timestamp_ntz, '2024-02-19 10:00:00'::timestamp_ntz, '2024-02-21 12:00:00'::timestamp_ntz, 'promo_002', 64.98),
    ('ord_007', 'usr_005', 'delivered',  '2024-03-01 13:00:00'::timestamp_ntz, '2024-03-02 14:00:00'::timestamp_ntz, '2024-03-05 10:00:00'::timestamp_ntz, null,       139.97),
    ('ord_008', 'usr_002', 'processing', '2024-03-05 16:20:00'::timestamp_ntz, null,                                  null,                                  null,       119.99)
as t(order_id, user_id, status, created_at, shipped_at, delivered_at, promotion_id, total_amount);
```

#### order_items

```sql
create table example_2_database.source.order_items as
select order_item_id, order_id, product_id, quantity, unit_price, discount_amount
from values
    ('oi_001', 'ord_001', 'prod_001', 1,  79.99, null),
    ('oi_002', 'ord_001', 'prod_002', 1,  34.99, null),
    ('oi_003', 'ord_002', 'prod_003', 2,  29.99, null),
    ('oi_004', 'ord_002', 'prod_006', 1,  24.99, null),
    ('oi_005', 'ord_003', 'prod_004', 1,  89.99, 9.00),
    ('oi_006', 'ord_004', 'prod_005', 1, 119.99, null),
    ('oi_007', 'ord_004', 'prod_003', 1,  29.99, null),
    ('oi_008', 'ord_005', 'prod_001', 1,  79.99, null),
    ('oi_009', 'ord_006', 'prod_002', 2,  34.99, 5.00),
    ('oi_010', 'ord_007', 'prod_004', 1,  89.99, null),
    ('oi_011', 'ord_007', 'prod_006', 2,  24.99, null),
    ('oi_012', 'ord_008', 'prod_005', 1, 119.99, null)
as t(order_item_id, order_id, product_id, quantity, unit_price, discount_amount);
```

#### reviews

```sql
create table example_2_database.source.reviews as
select review_id, product_id, user_id, order_id, rating, title, body, created_at
from values
    ('rev_001', 'prod_001', 'usr_001', 'ord_001', 5, 'Amazing sound quality', 'Best headphones I have ever owned. Crystal clear audio.',               '2024-01-20 09:00:00'::timestamp_ntz),
    ('rev_002', 'prod_003', 'usr_002', 'ord_002', 4, 'Great mat, good grip',  'Very comfortable for yoga. Slightly thicker than expected.',             '2024-01-22 14:30:00'::timestamp_ntz),
    ('rev_003', 'prod_004', 'usr_001', 'ord_003', 3, 'Decent coffee maker',   'Makes good coffee but the carafe lid leaks a little.',                  '2024-01-27 10:15:00'::timestamp_ntz),
    ('rev_004', 'prod_005', 'usr_003', 'ord_004', 5, 'Perfect running shoes', 'Extremely comfortable. Great arch support for long runs.',               '2024-02-10 16:00:00'::timestamp_ntz),
    ('rev_005', 'prod_001', 'usr_004', 'ord_005', 2, 'Disappointed',         'Left earcup stopped working after two weeks. Expected better quality.',  '2024-02-15 11:30:00'::timestamp_ntz),
    ('rev_006', 'prod_002', 'usr_001', 'ord_006', 4, 'Solid USB hub',        'Works perfectly with my MacBook. All ports function as advertised.',     '2024-02-23 08:45:00'::timestamp_ntz)
as t(review_id, product_id, user_id, order_id, rating, title, body, created_at);
```

#### inventory

```sql
create table example_2_database.source.inventory as
select inventory_id, product_id, warehouse_id, quantity_on_hand, reorder_threshold, last_updated_at
from values
    ('inv_001', 'prod_001', 'wh_east',    45,  10, '2024-03-05 06:00:00'::timestamp_ntz),
    ('inv_002', 'prod_002', 'wh_east',     8,  15, '2024-03-05 06:00:00'::timestamp_ntz),
    ('inv_003', 'prod_003', 'wh_west',   120,  20, '2024-03-05 06:00:00'::timestamp_ntz),
    ('inv_004', 'prod_004', 'wh_central',  0,   5, '2024-03-05 06:00:00'::timestamp_ntz),
    ('inv_005', 'prod_005', 'wh_west',    32,  10, '2024-03-05 06:00:00'::timestamp_ntz),
    ('inv_006', 'prod_006', 'wh_central', 67,  20, '2024-03-05 06:00:00'::timestamp_ntz)
as t(inventory_id, product_id, warehouse_id, quantity_on_hand, reorder_threshold, last_updated_at);
```

#### events

```sql
create table example_2_database.source.events as
select event_id, user_id, event_type, occurred_at, parse_json(properties) as properties
from values
    ('evt_001', 'usr_001', 'page_view',    '2024-01-15 09:45:00'::timestamp_ntz, '{"page":"/home","referrer":"google.com"}'),
    ('evt_002', 'usr_001', 'product_view', '2024-01-15 09:47:00'::timestamp_ntz, '{"product_id":"prod_001","page":"/products/prod_001"}'),
    ('evt_003', 'usr_001', 'add_to_cart',  '2024-01-15 09:50:00'::timestamp_ntz, '{"product_id":"prod_001","cart_value":79.99}'),
    ('evt_004', 'usr_001', 'purchase',     '2024-01-15 10:00:00'::timestamp_ntz, '{"product_id":"prod_001","cart_value":114.98}'),
    ('evt_005', 'usr_002', 'page_view',    '2024-01-16 14:00:00'::timestamp_ntz, '{"page":"/sports","referrer":null}'),
    ('evt_006', 'usr_002', 'product_view', '2024-01-16 14:03:00'::timestamp_ntz, '{"product_id":"prod_003","page":"/products/prod_003"}'),
    ('evt_007', 'usr_002', 'add_to_cart',  '2024-01-16 14:08:00'::timestamp_ntz, '{"product_id":"prod_003","cart_value":59.98}'),
    ('evt_008', 'usr_003', 'page_view',    '2024-02-03 13:30:00'::timestamp_ntz, '{"page":"/sports","referrer":"instagram.com"}'),
    ('evt_009', 'usr_003', 'product_view', '2024-02-03 13:33:00'::timestamp_ntz, '{"product_id":"prod_005","page":"/products/prod_005"}'),
    ('evt_010', 'usr_003', 'purchase',     '2024-02-03 14:00:00'::timestamp_ntz, '{"product_id":"prod_005","cart_value":149.98}')
as t(event_id, user_id, event_type, occurred_at, properties);
```

#### returns

```sql
create table example_2_database.source.returns as
select return_id, order_item_id, user_id, reason, status, created_at, refund_amount
from values
    ('ret_001', 'oi_003', 'usr_002', 'Item arrived with a defect — zipper broken', 'refunded',  '2024-01-23 10:00:00'::timestamp_ntz, 29.99),
    ('ret_002', 'oi_005', 'usr_001', 'Changed mind, no longer need it',            'refunded',  '2024-01-28 14:30:00'::timestamp_ntz, 80.99)
as t(return_id, order_item_id, user_id, reason, status, created_at, refund_amount);
```

---

## Running the example

From the project root (`clair/`):

```bash
# Compile (offline — resolves the DAG and prints generated SQL)
clair compile --project example_projects/example_2

# Run (executes against Snowflake)
clair run --project example_projects/example_2 --profile local
```

After running, 40 new tables will be created across three schemas. A few interesting ones to verify:

```sql
-- Silver layer: VARIANT flattened into typed columns
select * from example_2_database.refined.users;
select * from example_2_database.refined.events;

-- Derived: aggregations
select * from example_2_database.derived.user_order_stats order by lifetime_value desc;
select * from example_2_database.derived.product_sales_summary order by net_revenue desc;
select * from example_2_database.derived.user_funnel order by event_date;

-- Reports: business intelligence
select * from example_2_database.reports.top_customers;
select * from example_2_database.reports.churned_users;
select * from example_2_database.reports.inventory_alerts;  -- prod_002 low stock, prod_004 out of stock
select * from example_2_database.reports.high_return_customers;
select * from example_2_database.reports.trending_categories;
```
