from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2024, 1, 1),
}

dag = DAG(
    'hive_to_clickhouse_parametrized',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
)

run_spark_job = BashOperator(
    task_id='run_spark_parametrized',
    bash_command="""
    yc dataproc job submit pyspark \
      --cluster-name=my-cluster \
      --bucket=my-spark-jobs \
      --name=hive-to-ch-dag \
      -- \
      s3a://my-spark-jobs/hive_to_clickhouse.py \
      my_hive_db my_hive_table \
      analytics_db sales_data \
      {{ var.value.CH_HOST }} \
      {{ var.value.CH_USER }} \
      {{ var.value.CH_PASS }} \
      {{ var.value.CH_CA_CERT_PATH }}
    """,
    dag=dag
)

run_spark_job
