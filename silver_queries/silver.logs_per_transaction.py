spark.sql("CREATE DATABASE IF NOT EXISTS silver")
spark.sql("DROP TABLE IF EXISTS silver.logs_per_transaction")
spark.sql("""
    CREATE TABLE silver.logs_per_transaction
    USING parquet
    AS
    SELECT 
      t.transaction_id,
      COUNT(l.log_id) AS log_count
    FROM bronze.transactions_v2 t
    LEFT JOIN bronze.logs_v2 l
      ON t.transaction_id = l.transaction_id
    GROUP BY t.transaction_id
""")