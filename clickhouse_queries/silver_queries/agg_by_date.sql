CREATE DATABASE IF NOT EXISTS silver;

DROP TABLE IF EXISTS silver.agg_by_date;

CREATE TABLE silver.agg_by_date
ENGINE = MergeTree()
ORDER BY tuple()
AS
SELECT
    toDate(order_date) AS order_day,
    COUNT(*) AS daily_orders,
    SUM(total_amount) AS daily_total
FROM bronze.orders
GROUP BY order_day
ORDER BY order_day;