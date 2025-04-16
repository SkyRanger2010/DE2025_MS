INSERT INTO gold.orders
SELECT
    order_id,
    user_id,
    parseDateTimeBestEffort(order_date) AS order_date,
    total_amount,
    payment_status
FROM s3(
    'https://storage.yandexcloud.net/data-proc-bucket/source/orders.csv',
    'CSVWithNames'
);