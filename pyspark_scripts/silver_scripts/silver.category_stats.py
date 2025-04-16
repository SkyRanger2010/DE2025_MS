spark.sql("CREATE DATABASE IF NOT EXISTS silver")
spark.sql("DROP TABLE IF EXISTS silver.category_stats")
spark.sql("""
    CREATE TABLE silver.category_stats
    USING parquet
    AS
    SELECT 
      category,
      COUNT(*) AS log_count
    FROM bronze.logs_v2
    GROUP BY category
    ORDER BY log_count DESC
""")