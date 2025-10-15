import os
from datetime import datetime
from pandas import pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, lit
from kaggle.api.kaggle_api_extended import KaggleApi
from alpha_vantage.timeseries import TimeSeries
import yfinance as yf
from dotenv import load_dotenv

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
    .config("spark.sql.catalog.iceberg_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

today = datetime.utcnow().date()

# Hàm ghi DataFrame vào Iceberg
def write_iceberg(df: pd.DataFrame, table_full_name: str):
    if df is None or df.empty:
        print(f"⚠️  DataFrame rỗng, bỏ qua ghi vào {table_full_name}")
        return
    
    df['dt'] = df.get('dt', pd.Series([today]*len(df)))  
    sdf = spark.createDataFrame(df)
    
    # Thêm vào Iceberg table
    sdf.writeTo(table_full_name).append()
    print(f"✅ Ghi thành công {len(df)} bản ghi vào {table_full_name}")
    
# 1. Ingest Kaggle datasets
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

for name, info in datasets.items():
    dataset_dir = f"./data/{name}"
    os.makedirs(dataset_dir, exist_ok=True)
    file_path = os.path.join(dataset_dir, info['file'])

    if not os.path.exists(file_path):
        print(f"⚠️  File {file_path} không tồn tại, bỏ qua.")
        continue

    api.dataset_download_files(info['dataset'], path=dataset_dir, unzip=True)
    df = pd.read_csv(file_path, encoding="utf-8", low_memory=False)
    df['dt'] = today
    write_iceberg(df, f"iceberg_catalog.finance.{name}")


# 2. Ingest Alpha Vantage stock data
av_key= os.getenv("ALPHA_VANTAGE_API_KEY")
if av_key:
    ts = TimeSeries(key=av_key, output_format="pandas")
    symbols = ["AAPL", "MSFT", "GOOGL"]
    for symbol in symbols:
        data, _ = ts.get_daily(symbol=symbol, outputsize="compact")
        data.reset_index(inplace=True)
        data['dt'] = today
        write_iceberg(data, f"iceberg_catalog.market.av_{symbol}_quotes")
        
# 3. Ingest Yahoo Finance stock data
symbols = ["AAPL", "MSFT", "GOOGL"]
try:
    yf_df = yf.download(symbols, start="2025-01-01", end=str(today))
    yf_df.reset_index(inplace=True)
    yf_df['dt'] = today
    write_iceberg(yf_df, "iceberg_catalog.market.yfinance_quotes")
except Exception as e:
    print(f"⚠️  Lỗi khi tải dữ liệu từ Yahoo Finance: {e}")


# Kết thúc Spark session
spark.stop()
print("✅ Hoàn tất ingest batch data vào Iceberg")