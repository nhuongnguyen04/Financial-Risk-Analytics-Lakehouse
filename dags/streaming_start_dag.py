from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 10, 17),
    'retries': 0,  # Không retry cho job dài
}

dag = DAG(
    'start_streaming_to_iceberg',
    default_args=default_args,
    description='Khởi động streaming từ Kafka vào Iceberg',
    schedule_interval=None,  # Manual
    catchup=False,
)

stream_task = BashOperator(
    task_id='start_stream_to_iceberg',
    bash_command='spark-submit /opt/airflow/scripts/stream_to_iceberg.py',  # Chạy trong background nếu cần nohup
    dag=dag,
)