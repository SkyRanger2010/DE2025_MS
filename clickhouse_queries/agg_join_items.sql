SELECT
    COUNT(oi.item_id) AS total_items,
    SUM(oi.quantity) AS total_quantity,
    SUM(oi.quantity * oi.product_price) AS total_revenue,
    AVG(oi.product_price) AS avg_product_price
FROM orders o
JOIN order_items oi ON o.order_id = oi.order_id;