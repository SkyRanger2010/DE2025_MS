spark.sql("CREATE DATABASE IF NOT EXISTS silver")
spark.sql("DROP TABLE IF EXISTS silver.daily_summary")
spark.sql("""
    CREATE TABLE silver.daily_summary
    USING parquet
    AS
    SELECT 
      DATE(transaction_date) AS date,
      COUNT(*) AS transaction_count,
      SUM(amount) AS total_amount,
      ROUND(AVG(amount), 2) AS avg_amount
    FROM bronze.transactions_v2
    GROUP BY DATE(transaction_date)
    ORDER BY date
""")