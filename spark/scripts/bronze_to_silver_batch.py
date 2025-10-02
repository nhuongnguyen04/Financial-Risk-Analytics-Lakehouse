from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_utc_timestamp, sha2, to_date
from datetime import datetime

# Khởi tạo SparkSession với Delta Lake
spark = SparkSession.builder \
    .appName("BronzeToSilverBatchRefactored") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "12345678") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# Danh sách bảng và domain
batch_tables = [
    ("paysim_transactions", "finance"),
    ("credit_card_transactions", "finance"),
    ("bank_customers", "finance"),
    ("german_credit", "finance"),
    ("loghub_logs", "system"),
    ("av_aapl_quotes", "market"),
    ("av_googl_quotes", "market"),
    ("av_msft_quotes", "market"),
    ("yfinance_quotes", "market"),
    ("ofac_sanctions", "risk")
]

# Cấu hình theo bảng
key_columns = {
    "paysim_transactions": "step",
    "credit_card_transactions": "trans_num",
    "bank_customers": "customerid",
    "german_credit": None,
    "loghub_logs": "eventid",
    "av_aapl_quotes": "date",
    "av_googl_quotes": "date",
    "av_msft_quotes": "date",
    "yfinance_quotes": "date",
    "ofac_sanctions": None
}

timestamp_columns = {
    "paysim_transactions": "trans_date_trans_time",
    "credit_card_transactions": "trans_date_trans_time",
    "bank_customers": None,
    "german_credit": None,
    "loghub_logs": "date",
    "av_aapl_quotes": "date",
    "av_googl_quotes": "date",
    "av_msft_quotes": "date",
    "yfinance_quotes": "date",
    "ofac_sanctions": None
}

pii_columns = {
    "paysim_transactions": ["nameorig", "namedest"],
    "credit_card_transactions": ["cc_num", "first", "last"],
    "bank_customers": ["customerid", "name"],
    "german_credit": ["personal_status"],
    "loghub_logs": None,
    "av_aapl_quotes": None,
    "av_googl_quotes": None,
    "av_msft_quotes": None,
    "yfinance_quotes": None,
    "ofac_sanctions": ["name"]
}

# Hàm chuẩn hóa tên cột
def to_snake_case(df):
    for col_name in df.columns:
        new_col_name = col_name.lower().replace(" ", "_").replace("-", "_")
        df = df.withColumnRenamed(col_name, new_col_name)
    return df

# Ngày xử lý batch
today = datetime.now().strftime("%Y-%m-%d")

# Xử lý từng bảng
for table, domain in batch_tables:
    bronze_path = f"s3a://lake/bronze/{domain}/{table}/dt={today}"
    silver_path = f"s3a://lake/silver/{domain}/{table}"

    try:
        df = spark.read.parquet(bronze_path)

        # Deduplication
        if key_columns[table]:
            df = df.dropDuplicates([key_columns[table]])

        # Timestamp normalization
        ts_col = timestamp_columns[table]
        if ts_col and ts_col in df.columns:
            df = df.withColumn(ts_col, to_utc_timestamp(col(ts_col), "UTC")) \
                   .withColumn("dt", to_date(col(ts_col)))

        # PII hashing
        if pii_columns[table]:
            for pii in pii_columns[table]:
                if pii in df.columns:
                    df = df.withColumn(f"{pii}_hashed", sha2(col(pii), 256)) \
                           .drop(pii) \
                           .withColumnRenamed(f"{pii}_hashed", pii)

        # Chuẩn hóa tên cột
        df = to_snake_case(df)

        # Ghi dữ liệu vào Silver layer
        df.write.format("delta").partitionBy("dt").mode("append").save(silver_path)

        # Tạo bảng Delta nếu chưa có
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS silver_{domain}_{table}
            USING DELTA
            LOCATION '{silver_path}'
        """)

        print(f"✅ Đã xử lý {domain}/{table} thành công.")
    except Exception as e:
        print(f"⚠️ Lỗi xử lý {domain}/{table}: {e}")

# Dừng SparkSession
spark.stop()