spark.sql("CREATE DATABASE IF NOT EXISTS silver")
spark.sql("DROP TABLE IF EXISTS silver.total_by_currency")
spark.sql("""
    CREATE TABLE silver.total_by_currency
    USING parquet
    AS
    SELECT 
      currency,
      COUNT(*) AS transaction_count,
      SUM(amount) AS total_amount
    FROM bronze.transactions_v2
    WHERE currency IN ('USD', 'EUR', 'RUB')
    GROUP BY currency
""")