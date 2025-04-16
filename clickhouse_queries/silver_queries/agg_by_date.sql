SELECT
    toDate(order_date) AS order_day,
    COUNT(*) AS daily_orders,
    SUM(total_amount) AS daily_total
FROM orders
GROUP BY order_day
ORDER BY order_day;