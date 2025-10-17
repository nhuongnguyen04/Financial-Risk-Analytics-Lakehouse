from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'NGUYEN MANH NHUONG',
    'depends_on_past': False,
    'start_date': datetime(2025, 10, 17),
    'retries': 1,
}

dag = DAG(
    'iceberg_init',
    default_args=default_args,
    description='Khởi tạo Iceberg catalog',
    schedule_interval=None,  # Manual trigger
    catchup=False,
)

init_task = BashOperator(
    task_id='init_iceberg_catalog',
    bash_command='spark-submit /opt/airflow/scripts/init_iceberg_catalog.py',
    dag=dag,
)