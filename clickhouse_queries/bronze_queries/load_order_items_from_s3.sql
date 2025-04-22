INSERT INTO bronze.order_items
SELECT
    item_id,
    order_id,
    product_name,
    product_price,
    quantity
FROM s3(
    'https://storage.yandexcloud.net/data-proc-backet/source/order_items.txt',
    'CSVWithNames'
) SETTINGS format_csv_delimiter = ';';
