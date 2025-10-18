#!/bin/bash
set -e

# This script can be used to initialize Airflow with any additional setup if needed.
# Currently, it does not perform any actions but can be extended in the future.
if [ -z "${AIRFLOW_UID}" ]; then
    export AIRFLOW_UID=$(id -u)
fi

mkdir -pv /opt/airflow/{logs,dags,plugins}
chown -R "${AIRFLOW_UID}:0" /opt/airflow/

echo "Running Airflow initialization..."
/entrypoint airflow db migrate

exec /entrypoint airflow users create \
    --username ${_AIRFLOW_WWW_USER_USERNAME:-airflow} \
    --firstname Airflow \
    --lastname Admin \
    --role Admin \
    --email admin@example.com \
    --password ${_AIRFLOW_WWW_USER_PASSWORD:-airflow}
exec /entrypoint airflow connections add 'minio_default' \
    --conn-type 's3' \
    --conn-login '${MINIO_ROOT_USER:-minioadmin}' \
    --conn-password '${MINIO_ROOT_PASSWORD:-minioadmin}' \
    --conn-host 'http://minio:9000' 
exec /entrypoint airflow connections add 'kafka_default' \
    --conn-type 'kafka' \
    --conn-host 'kafka:9092'
exec /entrypoint airflow connections add 'spark_default' \
    --conn-type 'spark' \
    --conn-host 'spark://spark:7077'
