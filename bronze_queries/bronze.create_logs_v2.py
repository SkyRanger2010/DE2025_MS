# === Импорт необходимых функций ===
from pyspark.sql.functions import col
from pyspark.sql.types import TimestampType

# === Чтение CSV-файла ===
df = spark.read.option("header", True).option("delimiter", ";").option("inferSchema", True).csv("s3a://data-proc-backet/source/logs_v2.txt")

# === Преобразование даты ===
df = df.withColumn("log_timestamp", col("log_timestamp").cast(TimestampType()))

# === Создание базы bronze (если нет) ===
spark.sql("CREATE DATABASE IF NOT EXISTS bronze")

# === Запись в Hive-таблицу ===
df.write.mode("overwrite").format("parquet").saveAsTable("bronze.logs_v2")

# === Проверка результата ===
spark.sql("SELECT * FROM bronze.logs_v2 LIMIT 5").show()
