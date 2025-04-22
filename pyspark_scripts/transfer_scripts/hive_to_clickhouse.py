import sys
from pyspark.sql import SparkSession
from clickhouse_driver import Client
import logging


def spark_type_to_ch(spark_type):
    # Простое соответствие Spark → ClickHouse типов
    mapping = {
        'StringType': 'String',
        'IntegerType': 'Int32',
        'LongType': 'Int64',
        'DoubleType': 'Float64',
        'FloatType': 'Float32',
        'BooleanType': 'UInt8',
        'TimestampType': 'DateTime',
        'DateType': 'Date'
    }
    return mapping.get(spark_type, 'String')  # по умолчанию String


def create_clickhouse_table(client, df, ch_db, ch_table):
    schema = df.schema
    columns = []
    for field in schema.fields:
        ch_type = spark_type_to_ch(field.dataType.simpleString())
        columns.append(f"`{field.name}` {ch_type}")
    columns_ddl = ",\n".join(columns)

    ddl = f"""
    CREATE TABLE IF NOT EXISTS {ch_db}.{ch_table} (
        {columns_ddl}
    ) ENGINE = MergeTree()
    ORDER BY tuple()
    """
    logging.info("Creating ClickHouse table if not exists...")
    client.execute(ddl)


def write_to_clickhouse(df, ch_host, ch_db, ch_table, ch_user, ch_pass, ch_port=9440, ca_cert=None):
    client = Client(
        host=ch_host,
        port=ch_port,
        user=ch_user,
        password=ch_pass,
        secure=True,
        verify=True,
        ca_certs=ca_cert
    )

    # Создаем таблицу, если её нет
    create_clickhouse_table(client, df, ch_db, ch_table)

    # Преобразуем Spark DataFrame в список кортежей
    data = [tuple(row) for row in df.collect()]
    insert_query = f"INSERT INTO {ch_db}.{ch_table} VALUES"
    client.execute(insert_query, data)


if __name__ == "__main__":
    # Аргументы из командной строки
    hive_db = sys.argv[1]
    hive_table = sys.argv[2]
    ch_db = sys.argv[3]
    ch_table = sys.argv[4]
    ch_host = sys.argv[5]
    ch_user = sys.argv[6]
    ch_pass = sys.argv[7]
    ca_cert = sys.argv[8] if len(sys.argv) > 8 else None

    spark = SparkSession.builder \
        .appName("HiveToClickHouse") \
        .enableHiveSupport() \
        .getOrCreate()

    # Загружаем таблицу из Hive
    df = spark.sql(f"SELECT * FROM {hive_db}.{hive_table}")

    # Пишем в ClickHouse
    write_to_clickhouse(df, ch_host, ch_db, ch_table, ch_user, ch_pass, ca_cert=ca_cert)

    spark.stop()
