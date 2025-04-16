CREATE DATABASE IF NOT EXISTS bronze
CREATE TABLE IF NOT EXISTS bronze.orders (
    order_id UInt64,
    user_id UInt64,
    order_date DateTime,
    total_amount Float64,
    payment_status String
) ENGINE = MergeTree()
ORDER BY order_id;
