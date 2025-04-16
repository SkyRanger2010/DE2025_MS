CREATE DATABASE IF NOT EXISTS gold;
CREATE TABLE IF NOT EXISTS gold.order_items (
    item_id UInt64,
    order_id UInt64,
    product_name String,
    product_price Float64,
    quantity UInt32
) ENGINE = MergeTree()
ORDER BY item_id;