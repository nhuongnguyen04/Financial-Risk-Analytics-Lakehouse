from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'NGUYEN MANH NHUONG',
    'depends_on_past': False,
    'start_date': datetime(2025, 10, 17),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'batch_ingest_transactions',
    default_args=default_args,
    description='Ingest batch transactions từ Kaggle vào Iceberg',
    schedule_interval='@daily',  # Chạy hàng ngày
    catchup=False,
)

ingest_task = BashOperator(
    task_id='ingest_batch_to_iceberg',
    bash_command='spark-submit /opt/airflow/scripts/ingest_batch_transaction_to_iceberg.py',
    dag=dag,
)