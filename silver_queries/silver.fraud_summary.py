spark.sql("CREATE DATABASE IF NOT EXISTS silver")
spark.sql("DROP TABLE IF EXISTS silver.fraud_summary")
spark.sql("""
    CREATE TABLE silver.fraud_summary
    USING parquet
    AS
    SELECT 
      is_fraud,
      COUNT(*) AS count_transactions,
      SUM(amount) AS total_amount,
      ROUND(AVG(amount), 2) AS avg_amount
    FROM bronze.transactions_v2
    GROUP BY is_fraud
""")