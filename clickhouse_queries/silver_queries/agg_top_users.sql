CREATE DATABASE IF NOT EXISTS silver;

DROP TABLE IF EXISTS silver.agg_top_users;

CREATE TABLE silver.agg_top_users
ENGINE = MergeTree()
ORDER BY tuple()
AS
SELECT
    user_id,
    COUNT(*) AS order_count,
    SUM(total_amount) AS total_spent
FROM bronze.orders
GROUP BY user_id
ORDER BY total_spent DESC
LIMIT 10;