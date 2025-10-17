from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.bash import BashSensor
from datetime import datetime, timedelta

default_args = {
    'owner': 'NGUYEN MANH NHUONG',
    'start_date': datetime(2023, 1, 1),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'bronze_to_silver_etl',
    default_args=default_args,
    description='ETL process from bronze to silver layer',
    schedule_interval=None,
    catchup=False,
)

start_transactions = BashOperator(
    task_id='start_bronze_to_silver_transactions',
    bash_command='nohup spark-submit /opt/airflow/scripts/silver/transform_transactions.py > /opt/airflow/logs/bronze_to_silver_transactions.log 2>&1 &',
    dag=dag,
)

start_auth = BashOperator(
    task_id='start_bronze_to_silver_auth',
    bash_command='nohup spark-submit /opt/airflow/scripts/silver/transform_auth_logs.py > /opt/airflow/logs/bronze_to_silver_auth.log 2>&1 &',
    dag=dag,
)

start_system = BashOperator(
    task_id='start_bronze_to_silver_system',
    bash_command='nohup spark-submit /opt/airflow/scripts/silver/transform_system_logs.py > /opt/airflow/logs/bronze_to_silver_system.log 2>&1 &',
    dag=dag,
)

health_check = BashSensor(
    task_id='check_bronze_to_silver_health',
    bash_command='ps aux | grep "bronze_to_silver_etl" | grep -v grep | wc -l > /tmp/count.txt && [ $(cat /tmp/count.txt) -ge 3 ]',
    poke_interval=30,  # Check mỗi 30s
    dag=dag,
)

[start_transactions, start_auth, start_system] >> health_check
