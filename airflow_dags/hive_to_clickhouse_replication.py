import csv
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from pyhive import hive
from clickhouse_driver import Client

default_args = {
    'owner': 'Nikita Litvinov',
    'retries': 0,
}

dag = DAG(
    'hive_to_clickhouse_replication',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    start_date=days_ago(1)
)

def export_from_hive(**kwargs):
    hive_host = "89.169.132.254"
    hive_port = 10000
    hive_username = "ubuntu"
    hive_database = "mentor_db"

    conn = hive.Connection(host=hive_host, port=hive_port, username=hive_username, database=hive_database)
    cur = conn.cursor()

    query = """
    SELECT TO_DATE(transaction_date) AS transaction_day, 
           COUNT(*) AS transaction_count, 
           SUM(amount) AS total_amount, 
           AVG(amount) AS avg_amount 
    FROM transactions_v2 
    GROUP BY TO_DATE(transaction_date)
    """

    cur.execute(query)
    results = cur.fetchall()

    with open("/tmp/hive_export.csv", "w", newline="") as f:
        csv_writer = csv.writer(f)
        csv_writer.writerow(["transaction_day", "transaction_count", "total_amount", "avg_amount"])
        csv_writer.writerows(results)

    cur.close()
    conn.close()


export_hive_task = PythonOperator(
    task_id="export_from_hive",
    python_callable=export_from_hive,
    dag=dag
)


def create_or_update_table_driver(**kwargs):
    client = Client(
        host='rc1a-13d5vimannvvffe2.mdb.yandexcloud.net',
        user='admin',
        password='Fydx8353_',
        port=9440,
        secure=True,
        verify=True,
        ca_certs='/opt/airflow/ssh/RootCA.crt'
    )

    create_table_query = """
    CREATE TABLE IF NOT EXISTS db.transactions_new (
        transaction_day Date,
        transaction_count UInt64,
        total_amount Decimal(10,2),
        avg_amount Decimal(10,2)
    ) ENGINE = MergeTree() ORDER BY transaction_day
    """
    client.execute(create_table_query)

    truncate_query = "TRUNCATE TABLE db.transactions_new"
    client.execute(truncate_query)


def load_data_to_clickhouse(**kwargs):
    import csv
    from datetime import datetime

    data = []
    with open("/tmp/hive_export.csv", newline="") as csvfile:
        reader = csv.reader(csvfile)
        header = next(reader)
        for row in reader:
            if len(row) < 4:
                continue
            date_str = row[0].strip()
            if not date_str:
                continue
            try:
                transaction_day = datetime.strptime(date_str, "%Y-%m-%d").date()
            except ValueError:
                try:
                    transaction_day = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S").date()
                except ValueError:
                    continue
            transaction_count = int(row[1].strip()) if row[1].strip() else 0
            total_amount = float(row[2].strip()) if row[2].strip() else 0.0
            avg_amount = float(row[3].strip()) if row[3].strip() else 0.0
            data.append((transaction_day, transaction_count, total_amount, avg_amount))

    if data:
        client = Client(
            host='rc1a-13d5vimannvvffe2.mdb.yandexcloud.net',
            user='admin',
            password='Fydx8353_',
            port=9440,
            secure=True,
            verify=True,
            ca_certs='/opt/airflow/ssh/RootCA.crt'
        )
        insert_query = "INSERT INTO db.transactions_new (transaction_day, transaction_count, total_amount, avg_amount) VALUES"
        client.execute(insert_query, data)


create_table_task = PythonOperator(
    task_id='create_or_update_table_driver',
    python_callable=create_or_update_table_driver,
    dag=dag
)


load_data_task = PythonOperator(
    task_id='load_data_to_clickhouse',
    python_callable=load_data_to_clickhouse,
    dag=dag
)

export_hive_task >> create_table_task >> load_data_task