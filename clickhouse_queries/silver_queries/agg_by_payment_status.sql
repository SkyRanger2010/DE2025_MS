CREATE DATABASE IF NOT EXISTS silver;

DROP TABLE IF EXISTS silver.agg_by_payment_status;

CREATE TABLE silver.agg_by_payment_status
ENGINE = MergeTree()
ORDER BY tuple()
AS
SELECT
    payment_status,
    COUNT(*) AS order_count,
    SUM(total_amount) AS total_sum,
    AVG(total_amount) AS avg_order
FROM bronze.orders
GROUP BY payment_status;