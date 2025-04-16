INSERT INTO gold.order_items
SELECT
    item_id,
    order_id,
    product_name,
    product_price,
    quantity
FROM s3(
    'https://storage.yandexcloud.net/data-proc-bucket/source/order_items.txt',
    'CSVWithNames'
) SETTINGS format_csv_delimiter = ';';