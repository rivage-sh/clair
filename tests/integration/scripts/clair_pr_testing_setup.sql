-- One-time Snowflake setup for the clair integration tests.
--
-- Run this script once, with ACCOUNTADMIN.
--
-- It makes:
--   * the database CLAIR_PR_TESTING,
--   * one golden schema for each example project, with the source tables,
--   * the role CLAIR_PR_TESTING_F, which reaches that database only,
--   * the user CLAIR_PR_TESTING_USER, which signs in with a key pair.
--
-- Each test run makes a schema PR_<number> in CLAIR_PR_TESTING, clones the
-- golden source tables into it, and drops the schema when the pull request
-- closes.
--
-- Put your public key in the RSA_PUBLIC_KEY line before you run the script:
--
--   openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM \
--       -out clair_pr_testing_f.p8 -nocrypt
--   openssl rsa -in clair_pr_testing_f.p8 -pubout -out clair_pr_testing_f.pub
--   base64 -i clair_pr_testing_f.p8 | tr -d '\n' | pbcopy   # the GitHub secret
--
-- Keep the private key out of the repository.

USE ROLE ACCOUNTADMIN;


-- ---------------------------------------------------------------------------
-- The warehouse, with a credit limit
-- ---------------------------------------------------------------------------

-- Snowflake bills 60 seconds minimum each time the warehouse resumes. An
-- AUTO_SUSPEND below 60 seconds thus costs money and saves none: a gap in the
-- test suite suspends the warehouse, and the next statement pays a full minute
-- again. Keep AUTO_SUSPEND at 60 seconds.
CREATE WAREHOUSE IF NOT EXISTS CLAIR_PR_TESTING_WH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE;

-- The repository is public. The limit protects the account if a workflow runs
-- more often than you expect.
--
-- An X-Small warehouse uses 1 credit for each hour. On AWS us-east-1 the
-- Standard edition asks 2 dollars for each credit, thus 10 credits is a limit
-- of 20 dollars for each month. The Enterprise edition asks 3 dollars, thus
-- use 6 credits there.
--
-- DO NOTIFY sends an email to each ACCOUNTADMIN user that has a verified
-- email address and that enabled the notifications in the profile. Without
-- both steps the trigger sends nothing.
CREATE RESOURCE MONITOR IF NOT EXISTS CLAIR_PR_TESTING_MONITOR
    WITH CREDIT_QUOTA = 10
    FREQUENCY = MONTHLY
    START_TIMESTAMP = IMMEDIATELY
    TRIGGERS ON 80 PERCENT DO NOTIFY
             ON 100 PERCENT DO SUSPEND
             ON 110 PERCENT DO SUSPEND_IMMEDIATE;

ALTER WAREHOUSE CLAIR_PR_TESTING_WH SET RESOURCE_MONITOR = CLAIR_PR_TESTING_MONITOR;


-- ---------------------------------------------------------------------------
-- The database and the golden schemas
-- ---------------------------------------------------------------------------

CREATE DATABASE IF NOT EXISTS CLAIR_PR_TESTING;

CREATE SCHEMA IF NOT EXISTS CLAIR_PR_TESTING.EXAMPLE_1;
CREATE SCHEMA IF NOT EXISTS CLAIR_PR_TESTING.EXAMPLE_2;
CREATE SCHEMA IF NOT EXISTS CLAIR_PR_TESTING.EXAMPLE_3;
CREATE SCHEMA IF NOT EXISTS CLAIR_PR_TESTING.EXAMPLE_4;


-- ---------------------------------------------------------------------------
-- The role and the user
-- ---------------------------------------------------------------------------

CREATE ROLE IF NOT EXISTS CLAIR_PR_TESTING_F;

GRANT USAGE, OPERATE ON WAREHOUSE CLAIR_PR_TESTING_WH TO ROLE CLAIR_PR_TESTING_F;

GRANT USAGE ON DATABASE CLAIR_PR_TESTING TO ROLE CLAIR_PR_TESTING_F;

-- The role makes the schema of each run, thus it owns that schema and it can
-- drop it.
GRANT CREATE SCHEMA ON DATABASE CLAIR_PR_TESTING TO ROLE CLAIR_PR_TESTING_F;

-- The role reads the golden schemas, and it clones the tables from them. A
-- clone needs SELECT on the source table.
GRANT USAGE ON ALL SCHEMAS IN DATABASE CLAIR_PR_TESTING TO ROLE CLAIR_PR_TESTING_F;
GRANT SELECT ON ALL TABLES IN DATABASE CLAIR_PR_TESTING TO ROLE CLAIR_PR_TESTING_F;
GRANT USAGE ON FUTURE SCHEMAS IN DATABASE CLAIR_PR_TESTING TO ROLE CLAIR_PR_TESTING_F;
GRANT SELECT ON FUTURE TABLES IN DATABASE CLAIR_PR_TESTING TO ROLE CLAIR_PR_TESTING_F;

-- A service user cannot sign in with a password, and it cannot use the web
-- interface. It uses the key pair only.
CREATE USER IF NOT EXISTS CLAIR_PR_TESTING_USER
    TYPE = SERVICE
    DEFAULT_ROLE = CLAIR_PR_TESTING_F
    DEFAULT_WAREHOUSE = CLAIR_PR_TESTING_WH
    COMMENT = 'The service user of the clair integration tests in GitHub Actions.'
    RSA_PUBLIC_KEY = 'PUT-THE-BODY-OF-clair_pr_testing_f.pub-HERE-ON-ONE-LINE';

GRANT ROLE CLAIR_PR_TESTING_F TO USER CLAIR_PR_TESTING_USER;


-- ---------------------------------------------------------------------------
-- The golden source tables of example_1
-- ---------------------------------------------------------------------------

CREATE OR REPLACE TABLE CLAIR_PR_TESTING.EXAMPLE_1.EVENTS AS
SELECT
    event_id,
    user_id,
    event_type,
    occurred_at,
    parse_json(properties) AS properties
FROM VALUES
    ('1', 'usr_abc', 'page_view',    '2024-01-15 08:23:11'::timestamp_ntz, '{"page": "/home", "referrer": "google.com"}'),
    ('2', 'usr_abc', 'button_click', '2024-01-15 08:24:05'::timestamp_ntz, '{"element": "signup_btn", "page": "/home"}'),
    ('3', 'usr_def', 'page_view',    '2024-01-15 09:01:33'::timestamp_ntz, '{"page": "/pricing", "referrer": null}'),
    ('4', 'usr_def', 'form_submit',  '2024-01-15 09:03:47'::timestamp_ntz, '{"form": "contact", "success": true}'),
    ('5', 'usr_ghi', 'purchase',     '2024-01-15 11:45:00'::timestamp_ntz, '{"item_id": "prod_99", "amount": 49.99, "currency": "USD"}')
AS t(event_id, user_id, event_type, occurred_at, properties);


-- ---------------------------------------------------------------------------
-- The golden source tables of example_2
-- ---------------------------------------------------------------------------

CREATE OR REPLACE TABLE CLAIR_PR_TESTING.EXAMPLE_2.SELLERS AS
SELECT seller_id, name, email, parse_json(contact_info) AS contact_info, joined_at, is_verified
FROM VALUES
    ('sel_001', 'TechGadgets Inc',     'hello@techgadgets.io',       '{"phone":"+1-800-111-2222","country":"US","website":"techgadgets.io"}',     '2021-03-10 09:00:00'::timestamp_ntz, true),
    ('sel_002', 'HomeEssentials Co',   'support@homeessentials.com', '{"phone":"+1-800-333-4444","country":"US","website":"homeessentials.com"}', '2020-07-22 11:30:00'::timestamp_ntz, true),
    ('sel_003', 'SportsPro LLC',       'info@sportspro.com',         '{"phone":"+1-800-555-6666","country":"US","website":"sportspro.com"}',      '2022-01-05 08:00:00'::timestamp_ntz, false)
AS t(seller_id, name, email, contact_info, joined_at, is_verified);

CREATE OR REPLACE TABLE CLAIR_PR_TESTING.EXAMPLE_2.USERS AS
SELECT user_id, email, first_name, last_name, created_at, parse_json(address) AS address, is_prime_member
FROM VALUES
    ('usr_001', 'alice@example.com',  'Alice',  'Johnson', '2022-06-01 10:00:00'::timestamp_ntz, '{"street":"123 Maple St","city":"New York","state":"NY","country":"US","zip":"10001"}',   true),
    ('usr_002', 'bob@example.com',    'Bob',    'Smith',   '2023-01-14 15:22:00'::timestamp_ntz, '{"street":"456 Oak Ave","city":"Los Angeles","state":"CA","country":"US","zip":"90001"}', false),
    ('usr_003', 'carol@example.com',  'Carol',  'White',   '2021-11-30 08:45:00'::timestamp_ntz, '{"street":"789 Pine Rd","city":"Austin","state":"TX","country":"US","zip":"73301"}',     true),
    ('usr_004', 'david@example.com',  'David',  'Brown',   '2023-05-18 12:00:00'::timestamp_ntz, '{"street":"321 Elm St","city":"Seattle","state":"WA","country":"US","zip":"98101"}',     false),
    ('usr_005', 'emma@example.com',   'Emma',   'Davis',   '2022-09-09 17:30:00'::timestamp_ntz, '{"street":"654 Cedar Blvd","city":"Chicago","state":"IL","country":"US","zip":"60601"}', true)
AS t(user_id, email, first_name, last_name, created_at, address, is_prime_member);

CREATE OR REPLACE TABLE CLAIR_PR_TESTING.EXAMPLE_2.PRODUCTS AS
SELECT product_id, seller_id, title, category, subcategory, price, parse_json(attributes) AS attributes, created_at
FROM VALUES
    ('prod_001', 'sel_001', 'Wireless Headphones', 'Electronics', 'Audio',       79.99,  '{"brand":"SoundWave","weight_kg":0.25,"color":"Black","material":"Plastic"}',   '2021-06-01 00:00:00'::timestamp_ntz),
    ('prod_002', 'sel_001', 'USB-C Hub 7-in-1',    'Electronics', 'Accessories', 34.99,  '{"brand":"SoundWave","weight_kg":0.12,"color":"Silver","material":"Aluminum"}', '2021-09-15 00:00:00'::timestamp_ntz),
    ('prod_003', 'sel_003', 'Yoga Mat Extra Thick','Sports',      'Fitness',     29.99,  '{"brand":"FlexFit","weight_kg":1.20,"color":"Purple","material":"TPE"}',        '2022-02-20 00:00:00'::timestamp_ntz),
    ('prod_004', 'sel_002', '12-Cup Coffee Maker', 'Kitchen',     'Appliances',  89.99,  '{"brand":"BrewMaster","weight_kg":2.40,"color":"Stainless","material":"Steel"}','2020-11-01 00:00:00'::timestamp_ntz),
    ('prod_005', 'sel_003', 'Running Shoes Pro',   'Sports',      'Footwear',    119.99, '{"brand":"FlexFit","weight_kg":0.60,"color":"Blue","material":"Mesh"}',         '2022-05-10 00:00:00'::timestamp_ntz),
    ('prod_006', 'sel_002', 'LED Desk Lamp',       'Office',      'Lighting',    24.99,  '{"brand":"BrightHome","weight_kg":0.45,"color":"White","material":"ABS"}',      '2021-01-12 00:00:00'::timestamp_ntz)
AS t(product_id, seller_id, title, category, subcategory, price, attributes, created_at);

CREATE OR REPLACE TABLE CLAIR_PR_TESTING.EXAMPLE_2.PROMOTIONS AS
SELECT promotion_id, code, discount_type, discount_value, parse_json(rules) AS rules, starts_at, ends_at
FROM VALUES
    ('promo_001', 'SAVE10',   'percentage', 10.0, '{"min_order_value":50.0,"eligible_categories":["Electronics","Kitchen"],"max_uses":1000}', '2024-01-01 00:00:00'::timestamp_ntz, '2024-12-31 23:59:59'::timestamp_ntz),
    ('promo_002', 'WELCOME5', 'flat',        5.0, '{"min_order_value":25.0,"eligible_categories":null,"max_uses":500}',                       '2024-01-01 00:00:00'::timestamp_ntz, '2024-06-30 23:59:59'::timestamp_ntz)
AS t(promotion_id, code, discount_type, discount_value, rules, starts_at, ends_at);

CREATE OR REPLACE TABLE CLAIR_PR_TESTING.EXAMPLE_2.ORDERS AS
SELECT order_id, user_id, status, created_at, shipped_at, delivered_at, promotion_id, total_amount
FROM VALUES
    ('ord_001', 'usr_001', 'delivered',  '2024-01-15 10:00:00'::timestamp_ntz, '2024-01-16 14:00:00'::timestamp_ntz, '2024-01-18 11:00:00'::timestamp_ntz, null,        114.98),
    ('ord_002', 'usr_002', 'delivered',  '2024-01-16 15:30:00'::timestamp_ntz, '2024-01-17 10:00:00'::timestamp_ntz, '2024-01-20 09:00:00'::timestamp_ntz, null,         84.97),
    ('ord_003', 'usr_001', 'delivered',  '2024-01-22 09:15:00'::timestamp_ntz, '2024-01-23 12:00:00'::timestamp_ntz, '2024-01-25 14:00:00'::timestamp_ntz, 'promo_001',  80.99),
    ('ord_004', 'usr_003', 'delivered',  '2024-02-03 14:00:00'::timestamp_ntz, '2024-02-04 11:00:00'::timestamp_ntz, '2024-02-07 16:00:00'::timestamp_ntz, null,        149.98),
    ('ord_005', 'usr_004', 'delivered',  '2024-02-10 08:30:00'::timestamp_ntz, '2024-02-11 09:00:00'::timestamp_ntz, '2024-02-13 14:30:00'::timestamp_ntz, null,         79.99),
    ('ord_006', 'usr_001', 'delivered',  '2024-02-18 11:45:00'::timestamp_ntz, '2024-02-19 10:00:00'::timestamp_ntz, '2024-02-21 12:00:00'::timestamp_ntz, 'promo_002',  64.98),
    ('ord_007', 'usr_005', 'delivered',  '2024-03-01 13:00:00'::timestamp_ntz, '2024-03-02 14:00:00'::timestamp_ntz, '2024-03-05 10:00:00'::timestamp_ntz, null,        139.97),
    ('ord_008', 'usr_002', 'processing', '2024-03-05 16:20:00'::timestamp_ntz, null,                                 null,                                 null,        119.99)
AS t(order_id, user_id, status, created_at, shipped_at, delivered_at, promotion_id, total_amount);

CREATE OR REPLACE TABLE CLAIR_PR_TESTING.EXAMPLE_2.ORDER_ITEMS AS
SELECT order_item_id, order_id, product_id, quantity, unit_price, discount_amount
FROM VALUES
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
AS t(order_item_id, order_id, product_id, quantity, unit_price, discount_amount);

CREATE OR REPLACE TABLE CLAIR_PR_TESTING.EXAMPLE_2.REVIEWS AS
SELECT review_id, product_id, user_id, order_id, rating, title, body, created_at
FROM VALUES
    ('rev_001', 'prod_001', 'usr_001', 'ord_001', 5, 'Amazing sound quality', 'Best headphones I have ever owned. Crystal clear audio.',              '2024-01-20 09:00:00'::timestamp_ntz),
    ('rev_002', 'prod_003', 'usr_002', 'ord_002', 4, 'Great mat, good grip',  'Very comfortable for yoga. Slightly thicker than expected.',           '2024-01-22 14:30:00'::timestamp_ntz),
    ('rev_003', 'prod_004', 'usr_001', 'ord_003', 3, 'Decent coffee maker',   'Makes good coffee but the carafe lid leaks a little.',                 '2024-01-27 10:15:00'::timestamp_ntz),
    ('rev_004', 'prod_005', 'usr_003', 'ord_004', 5, 'Perfect running shoes', 'Extremely comfortable. Great arch support for long runs.',             '2024-02-10 16:00:00'::timestamp_ntz),
    ('rev_005', 'prod_001', 'usr_004', 'ord_005', 2, 'Disappointed',          'Left earcup stopped working after two weeks. Expected better quality.','2024-02-15 11:30:00'::timestamp_ntz),
    ('rev_006', 'prod_002', 'usr_001', 'ord_006', 4, 'Solid USB hub',         'Works perfectly with my MacBook. All ports function as advertised.',   '2024-02-23 08:45:00'::timestamp_ntz)
AS t(review_id, product_id, user_id, order_id, rating, title, body, created_at);

CREATE OR REPLACE TABLE CLAIR_PR_TESTING.EXAMPLE_2.INVENTORY AS
SELECT inventory_id, product_id, warehouse_id, quantity_on_hand, reorder_threshold, last_updated_at
FROM VALUES
    ('inv_001', 'prod_001', 'wh_east',     45, 10, '2024-03-05 06:00:00'::timestamp_ntz),
    ('inv_002', 'prod_002', 'wh_east',      8, 15, '2024-03-05 06:00:00'::timestamp_ntz),
    ('inv_003', 'prod_003', 'wh_west',    120, 20, '2024-03-05 06:00:00'::timestamp_ntz),
    ('inv_004', 'prod_004', 'wh_central',   0,  5, '2024-03-05 06:00:00'::timestamp_ntz),
    ('inv_005', 'prod_005', 'wh_west',     32, 10, '2024-03-05 06:00:00'::timestamp_ntz),
    ('inv_006', 'prod_006', 'wh_central',  67, 20, '2024-03-05 06:00:00'::timestamp_ntz)
AS t(inventory_id, product_id, warehouse_id, quantity_on_hand, reorder_threshold, last_updated_at);

CREATE OR REPLACE TABLE CLAIR_PR_TESTING.EXAMPLE_2.EVENTS AS
SELECT event_id, user_id, event_type, occurred_at, parse_json(properties) AS properties
FROM VALUES
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
AS t(event_id, user_id, event_type, occurred_at, properties);

CREATE OR REPLACE TABLE CLAIR_PR_TESTING.EXAMPLE_2.RETURNS AS
SELECT return_id, order_item_id, user_id, reason, status, created_at, refund_amount
FROM VALUES
    ('ret_001', 'oi_003', 'usr_002', 'Item arrived with a defect — zipper broken', 'refunded', '2024-01-23 10:00:00'::timestamp_ntz, 29.99),
    ('ret_002', 'oi_005', 'usr_001', 'Changed mind, no longer need it',            'refunded', '2024-01-28 14:30:00'::timestamp_ntz, 80.99)
AS t(return_id, order_item_id, user_id, reason, status, created_at, refund_amount);


-- ---------------------------------------------------------------------------
-- The golden source tables of example_4
-- ---------------------------------------------------------------------------

CREATE OR REPLACE TABLE CLAIR_PR_TESTING.EXAMPLE_4.EVENTS AS
SELECT
    event_id,
    user_id,
    event_type,
    occurred_at,
    parse_json(properties) AS properties
FROM VALUES
    ('1', 'usr_abc', 'page_view',    '2024-01-15 08:23:11'::timestamp_ntz, '{"page": "/home", "referrer": "google.com"}'),
    ('2', 'usr_abc', 'button_click', '2024-01-15 08:24:05'::timestamp_ntz, '{"element": "signup_btn", "page": "/home"}'),
    ('3', 'usr_def', 'page_view',    '2024-01-15 09:01:33'::timestamp_ntz, '{"page": "/pricing", "referrer": null}'),
    ('4', 'usr_def', 'form_submit',  '2024-01-15 09:03:47'::timestamp_ntz, '{"form": "contact", "success": true}'),
    ('5', 'usr_ghi', 'purchase',     '2024-01-15 11:45:00'::timestamp_ntz, '{"item_id": "prod_99", "amount": 49.99, "currency": "USD"}')
AS t(event_id, user_id, event_type, occurred_at, properties);


-- ---------------------------------------------------------------------------
-- The golden source tables of example_3
-- ---------------------------------------------------------------------------
--
-- Each date is fixed and old. example_3_database.derived.recent_orders selects
-- `created_at > dateadd('day', -3, current_timestamp())`, thus no row below
-- reaches that window. The incremental test inserts its own rows with
-- current_timestamp(), and it then knows the exact number of new rows.

CREATE OR REPLACE TABLE CLAIR_PR_TESTING.EXAMPLE_3.ORDERS AS
SELECT order_id, customer_id, order_status, amount, created_at, updated_at
FROM VALUES
    ('ord_001', 'cust_a', 'delivered', 49.99,  '2024-01-05 09:00:00'::timestamp_ntz, '2024-01-07 12:00:00'::timestamp_ntz),
    ('ord_002', 'cust_b', 'delivered', 120.00, '2024-01-06 10:30:00'::timestamp_ntz, '2024-01-08 14:00:00'::timestamp_ntz),
    ('ord_003', 'cust_a', 'delivered', 35.50,  '2024-01-08 11:15:00'::timestamp_ntz, '2024-01-09 16:45:00'::timestamp_ntz),
    ('ord_004', 'cust_c', 'shipped',   89.00,  '2024-01-12 08:20:00'::timestamp_ntz, '2024-01-13 09:10:00'::timestamp_ntz),
    ('ord_005', 'cust_b', 'placed',    15.00,  '2024-01-14 17:05:00'::timestamp_ntz, '2024-01-14 17:05:00'::timestamp_ntz),
    ('ord_006', 'cust_a', 'placed',    200.00, '2024-01-14 18:40:00'::timestamp_ntz, '2024-01-14 18:40:00'::timestamp_ntz)
AS t(order_id, customer_id, order_status, amount, created_at, updated_at);


-- The account identifier, for the GitHub secret:
SELECT concat(current_organization_name(), '-', current_account_name()) AS account;
