from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col, lit
from datetime import datetime
import os
from dotenv import load_dotenv
from alpha_vantage.timeseries import TimeSeries
import yfinance as yf
import pandas as pd
from kaggle.api.kaggle_api_extended import KaggleApi

# Khởi tạo SparkSession
spark = SparkSession.builder \
    .appName("IngestToDeltaLake") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "12345678") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# Load biến môi trường
load_dotenv()
today = datetime.now().strftime("%Y-%m-%d")

# Hàm ghi vào Delta Lake
def write_delta(df, domain, table):
    df_spark = spark.createDataFrame(df)
    df_spark = df_spark.withColumn("dt", to_date(col("dt")) if "dt" in df_spark.columns else to_date(lit(today)))
    df_spark.write.format("delta").mode("overwrite").partitionBy("dt").save(f"s3a://lake/bronze/{domain}/{table}")

# 1. Kaggle datasets
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
    "bank_customers": {
        "dataset": "tomculihiddleston/bank-customer-data-in-vietnam",
        "file": "BankCustomerData.csv"
    },
    "german_credit": {
        "dataset": "uciml/german-credit",
        "file": "german_credit_data.csv"
    }
}

for table, info in datasets.items():
    dataset = info["dataset"]
    file_name = info["file"]
    dataset_dir = f"./data/{dataset.replace('/', '_')}"
    os.makedirs(dataset_dir, exist_ok=True)
    api.dataset_download_files(dataset, path=dataset_dir, unzip=True)
    df = pd.read_csv(os.path.join(dataset_dir, file_name), encoding="utf-8")
    df["dt"] = today
    write_delta(df, "finance", table)

# 2. Alpha Vantage
av_key = os.getenv("ALPHA_VANTAGE_API_KEY")
ts = TimeSeries(key=av_key, output_format="pandas")
for symbol in ["AAPL", "GOOGL", "MSFT"]:
    data, _ = ts.get_daily(symbol=symbol, outputsize="full")
    data.reset_index(inplace=True)
    data["dt"] = today
    write_delta(data, "market", f"av_{symbol}_quotes")

# 3. Yahoo Finance
yf_df = yf.download(["AAPL", "GOOGL", "MSFT"], start="2025-01-01", end="2025-09-29")
yf_df.reset_index(inplace=True)
yf_df["dt"] = today
write_delta(yf_df, "market", "yfinance_quotes")

# 4. Logs
loghub_df = pd.read_csv("./data/HDFS_2k.log_structured.csv", encoding="utf-8")
loghub_df["dt"] = today
write_delta(loghub_df, "system", "loghub_logs")

# 5. OFAC
ofac_df = pd.read_csv("https://www.treasury.gov/ofac/downloads/sdn.csv", header=None, encoding="utf-8")
ofac_df["dt"] = today
write_delta(ofac_df, "risk", "ofac_sanctions")

spark.stop()