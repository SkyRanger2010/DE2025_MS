INSERT INTO bronze.orders
SELECT
    order_id,
    user_id,
    order_date,
    total_amount,
    payment_status
FROM s3(
    'https://storage.yandexcloud.net/data-proc-backet/source/orders.csv',
    'CSVWithNames'
);
