# === Импорт необходимых функций ===
from pyspark.sql.functions import col
from pyspark.sql.types import TimestampType

# === Чтение CSV-файла ===
df = spark.read.option("header", True).option("inferSchema", True).csv("s3a://data-proc-backet/source/transactions_v2.csv")

# === Преобразование даты ===
df = df2.withColumn("transaction_date", col("transaction_date").cast(TimestampType()))

# === Создание базы bronze (если нет) ===
spark.sql("CREATE DATABASE IF NOT EXISTS bronze")

# === Запись в Hive-таблицу ===
df.write.mode("overwrite").format("parquet").saveAsTable("bronze.transactions_v2")

# === Проверка результата ===
spark.sql("SELECT * FROM bronze.transactions_v2 LIMIT 5").show()
