import os
import pandas as pd
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, lit
from kaggle.api.kaggle_api_extended import KaggleApi
from alpha_vantage.timeseries import TimeSeries
import yfinance as yf
from dotenv import load_dotenv

# Load biến môi trường từ .env
load_dotenv()
# Cấu hình Spark với Iceberg
spark = SparkSession.builder \
    .appName("ingest_batch_to_iceberg") \
    .config("spark.sql.catalog.iceberg_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.iceberg_catalog.type", "jdbc") \
    .config("spark.sql.catalog.iceberg_catalog.uri", "jdbc:postgresql://postgres:5432/iceberg_catalog") \
    .config("spark.sql.catalog.iceberg_catalog.jdbc.user", "iceberg") \
    .config("spark.sql.catalog.iceberg_catalog.jdbc.password", "iceberg") \
    .config("spark.sql.catalog.iceberg_catalog.warehouse", "s3a://lake/") \
    .config("spark.sql.catalog.iceberg_catalog.jdbc.schema-version", "V1") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

today = datetime.utcnow().date()

# Hàm ghi DataFrame vào Iceberg
def write_iceberg(sdf, table_full_name: str):
    if sdf is None or sdf.count() == 0:
        print(f"⚠️  DataFrame rỗng, bỏ qua ghi vào {table_full_name}")
        return
    
    if 'dt' not in sdf.columns:
        sdf = sdf.withColumn('dt', lit(today))
    
    # Thêm vào Iceberg table
    try:
        sdf.writeTo(table_full_name).append()
    except:
        # Nếu chưa có table, tạo mới
        sdf.writeTo(table_full_name).using("iceberg").createOrReplace()
    print(f"✅ Ghi thành công vào {table_full_name}, số bản ghi: {sdf.count()}")
    

# # 1. Ingest Kaggle datasets
api = KaggleApi()
api.authenticate()

datasets = {
    "paysim_transactions": {
        "dataset": "ealaxi/paysim1",
        "file": "PS_20174392719_1491204439457_log.csv"
    },
    "credit_card_transactions": {
        "dataset": "kartik2112/fraud-detection",
        "file": "fraudTrain.csv"
    },
}

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
data_dir = os.path.join(project_root, "data")

for name, info in datasets.items():
    dataset_dir = os.path.join(data_dir, name)
    os.makedirs(dataset_dir, exist_ok=True)
    file_path = os.path.join(dataset_dir, info['file'])

    if os.path.exists(file_path):
        print(f"📁 File {file_path} đã tồn tại, đọc dữ liệu.")
        df = pd.read_csv(file_path, encoding="utf-8", low_memory=False)
    else:
        print(f"⬇️ Tải dataset {info['dataset']}...")
        api.dataset_download_files(info['dataset'], path=dataset_dir, unzip=True)
    
    sdf = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("encoding", "utf-8") \
        .csv(file_path)
    
    sdf = sdf.withColumn('dt', lit(today))
    
    write_iceberg(sdf, f"iceberg_catalog.finance.{name}")

# Kết thúc Spark session
spark.stop()
print("✅ Hoàn tất ingest batch data vào Iceberg")