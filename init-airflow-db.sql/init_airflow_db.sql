-- Tạo DB cho Airflow
CREATE DATABASE airflow;
CREATE USER airflow WITH PASSWORD 'airflow';
GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;

-- Tạo DB cho Iceberg (nếu chưa có)
CREATE DATABASE IF NOT EXISTS iceberg_catalog;
CREATE USER IF NOT EXISTS iceberg WITH PASSWORD 'iceberg';
GRANT ALL PRIVILEGES ON DATABASE iceberg_catalog TO iceberg;