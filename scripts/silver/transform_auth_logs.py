from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, to_date, when, regexp_extract
from datetime import datetime

# Cấu hình Spark với streaming và Iceberg
spark = SparkSession.builder \
    .appName('stream_bronze_to_silver_transactions') \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
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
    
bronze_df = spark \
    .readStream \
    .format("iceberg") \
    .load("iceberg_catalog.bronze.finance.auth_logs") \
    .withWatermark("timestamp", "1 hour")  # Handle late data từ Kafka
transformed_df = bronze_df \
    .filter(col("event_id").isNotNull() & col("user_id").isNotNull()) \
    .withColumn("timestamp_clean", to_timestamp(col("timestamp"))) \
    .withColumn("dt", to_date(col("timestamp_clean"))) \
    .withColumn("action_clean", when(col("action").isin("login", "logout", "password_change", "account_lock", "two_factor_auth", "session_timeout", "permission_change", "data_access", "profile_update"), col("action")).otherwise("unknown")) \
    .withColumn("is_success", when(col("result") == "success", 1).otherwise(0)) \
    .withColumn("is_suspicious", when(col("result").contains("failure") & col("ip_address").rlike(r"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$"), 1).otherwise(0)) \
    .withColumn("ip_address_clean", when(col("ip_address").rlike(r"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$"), col("ip_address")).otherwise(None)) \
    .withColumn("geo_lat", regexp_extract(col("geo"), r"^(-?\d+\.?\d*),", 1).cast("double")) \
    .withColumn("geo_lon", regexp_extract(col("geo"), r",(-?\d+\.?\d+)$", 1).cast("double")) \
    .dropDuplicates(["event_id", "dt"]) \
    .select(    
        col("event_id"),
        col("user_id"),
        col("timestamp_clean").alias("timestamp"),
        col("action_clean").alias("action"),
        col("device_id"),
        col("session_id"),
        col("ip_address_clean").alias("ip_address"),
        col("geo"),
        col("geo_lat"),
        col("geo_lon"),
        col("is_success"),
        col("is_suspicious"),
        col("dt")
    )
# Function MERGE batch mới từ bronze vào silver
def merge_batch(df, epoch_id):
    if df.rdd.isEmpty():
        print(f"⚠️ Empty batch at epoch {epoch_id}")
        return
    df.createOrReplaceTempView("bronze_batch")
    spark.sql("""
        MERGE INTO iceberg_catalog.finance.auth_logs AS silver
        USING bronze_batch AS bronze
        ON silver.event_id = bronze.event_id AND silver.dt = bronze.dt
        WHEN MATCHED THEN
          UPDATE SET *
        WHEN NOT MATCHED THEN
          INSERT *
    """)
    print(f"✅ Batch {epoch_id} merged at {datetime.now()} with {df.count()} records")

query = transformed_df.repartition(10).writeStream \
    .foreachBatch(merge_batch) \
    .outputMode("append") \
    .option("checkpointLocation", "/opt/spark/checkpoints/stream_bronze_to_silver_auth_logs") \
    .trigger(processingTime="10 seconds") \
    .start()

query.awaitTermination()