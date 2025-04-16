spark.sql("CREATE DATABASE IF NOT EXISTS silver")
spark.sql("DROP TABLE IF EXISTS silver.time_analysis")
spark.sql("""
    CREATE TABLE silver.time_analysis
    USING parquet
    AS
    SELECT 
      YEAR(transaction_date) AS year,
      MONTH(transaction_date) AS month,
      DAY(transaction_date) AS day,
      COUNT(*) AS transaction_count,
      SUM(amount) AS total_amount
    FROM bronze.transactions_v2
    GROUP BY year, month, day
    ORDER BY year, month, day
""")