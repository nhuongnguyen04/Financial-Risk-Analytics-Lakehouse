# bronze_to_silver_stream.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sha2, to_utc_timestamp, from_json
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType

# ============ Spark Session ============
spark = SparkSession.builder \
    .appName("BronzeToSilverStream") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "12345678") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

# ============ Định nghĩa schema cho từng stream ============
schemas = {
    "transactions": StructType()
        .add("transaction_id", StringType())
        .add("customer_id", IntegerType())
        .add("amount", DoubleType())
        .add("timestamp", StringType())
        .add("merchant", StringType())
        .add("geo", StringType())
        .add("is_fraud", IntegerType()),

    "auth_logs": StructType()
        .add("event_id", StringType())
        .add("user_id", IntegerType())
        .add("timestamp", StringType())
        .add("action", StringType())
        .add("device_id", StringType())
        .add("ip_address", StringType())
        .add("geo", StringType()),

    "system_logs": StructType()
        .add("log_id", StringType())
        .add("timestamp", StringType())
        .add("service", StringType())
        .add("severity", StringType())
        .add("message", StringType())
}

# ============ Hàm transform chung ============
def transform_common(df, schema, key_cols):
    df = df.withColumn("json", from_json(col("value"), schema)).select("json.*")

    # Deduplication
    if all(k in df.columns for k in key_cols):
        df = df.dropDuplicates(key_cols)

    # Chuẩn hóa timestamp
    if "timestamp" in df.columns:
        df = df.withColumn("event_time", to_utc_timestamp(col("timestamp"), "UTC"))

    # Hash/anonymize PII
    for pii_field in ["customer_id", "device_id", "user_id"]:
        if pii_field in df.columns:
            df = df.withColumn(f"{pii_field}_hashed", sha2(col(pii_field).cast("string"), 256)).drop(pii_field)

    return df

# ============ Streaming pipelines ============
topics = {
    "transactions": ["transaction_id"],
    "auth_logs": ["event_id"],
    "system_logs": ["log_id"]
}

for topic, key_cols in topics.items():
    bronze_path = f"s3a://lake/bronze/{'finance' if topic=='transactions' else 'system'}/{topic}/*"
    silver_path = f"s3a://lake/silver/{'finance' if topic=='transactions' else 'system'}/{topic}"

    df = spark.readStream \
        .format("parquet") \
        .load(bronze_path)

    df = transform_common(df, schemas[topic], key_cols)

    query = df.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", f"s3a://lake/checkpoints/silver/{topic}") \
        .start(silver_path)

print("🚀 Streaming jobs started...")
spark.streams.awaitAnyTermination()
