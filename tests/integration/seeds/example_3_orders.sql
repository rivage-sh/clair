-- The source orders of example_3.
--
-- example_3_database.derived.recent_orders selects a 3 day window. The rows
-- must therefore be young at each run, thus this table has no golden copy: the
-- run makes it, and current_timestamp() gives the time of the run.
--
-- {physical_name} becomes the routed name of example_3_database.source.orders.

create or replace table {physical_name} as
select order_id, customer_id, order_status, amount, created_at, updated_at
from values
    ('ord_001', 'cust_a', 'delivered', 49.99,  dateadd('day', -10, current_timestamp()), dateadd('day', -8, current_timestamp())),
    ('ord_002', 'cust_b', 'delivered', 120.00, dateadd('day', -9,  current_timestamp()), dateadd('day', -7, current_timestamp())),
    ('ord_003', 'cust_a', 'delivered', 35.50,  dateadd('day', -7,  current_timestamp()), dateadd('day', -6, current_timestamp())),
    ('ord_004', 'cust_c', 'shipped',   89.00,  dateadd('day', -3,  current_timestamp()), dateadd('day', -2, current_timestamp())),
    ('ord_005', 'cust_b', 'placed',    15.00,  dateadd('day', -1,  current_timestamp()), dateadd('day', -1, current_timestamp())),
    ('ord_006', 'cust_a', 'placed',    200.00, dateadd('day', -1,  current_timestamp()), dateadd('day', -1, current_timestamp()))
as t(order_id, customer_id, order_status, amount, created_at, updated_at);
