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

# Đọc stream từ bronze table (capture appends real-time)
bronze_df = spark \
    .readStream \
    .format("iceberg") \
    .load("iceberg_catalog.finance.transactions") \
    .withWatermark("timestamp", "1 hour")  # Handle late data từ Kafka

# Transform real-time: Clean, enrich
transformed_df = bronze_df \
    .filter(col("transaction_id").isNotNull() & col("customer_id").isNotNull()) \
    .withColumn("timestamp_clean", to_timestamp(col("timestamp"))) \
    .withColumn("dt", to_date(col("timestamp_clean"))) \
    .withColumn("amount_clean", when(col("amount") <= 0, None).otherwise(col("amount"))) \
    .filter(col("amount_clean").isNotNull()) \
    .withColumn("geo_lat", regexp_extract(col("geo"), r"^(-?\d+\.?\d*),", 1).cast("double")) \
    .withColumn("geo_lon", regexp_extract(col("geo"), r",(-?\d+\.?\d+)$", 1).cast("double")) \
    .withColumn("is_fraud_clean", when(col("is_fraud") == 1, 1).otherwise(0)) \
    .dropDuplicates(["transaction_id", "dt"]) \
    .select(
        col("transaction_id"),
        col("customer_id"),
        col("amount_clean").alias("amount"),
        col("timestamp_clean").alias("timestamp"),
        col("merchant"),
        col("geo"),
        col("geo_lat"),
        col("geo_lon"),
        col("is_fraud_clean").alias("is_fraud"),
        col("dt")
    )

# Function MERGE batch mới từ bronze vào silver
def merge_batch(df, epoch_id):
    if df.rdd.isEmpty():
        print(f"⚠️ Empty batch at epoch {epoch_id}")
        return
    df.createOrReplaceTempView("bronze_batch")
    
    # MERGE để upsert (dedup/update nếu ID tồn tại)
    spark.sql("""
        MERGE INTO iceberg_catalog.silver.finance.transactions AS target
        USING bronze_batch AS source
        ON target.transaction_id = source.transaction_id AND target.dt = source.dt
        WHEN MATCHED THEN 
            UPDATE SET 
                customer_id = source.customer_id,
                amount = source.amount,
                timestamp = source.timestamp,
                merchant = source.merchant,
                geo = source.geo,
                geo_lat = source.geo_lat,
                geo_lon = source.geo_lon,
                is_fraud = source.is_fraud,
                dt = source.dt
        WHEN NOT MATCHED THEN 
            INSERT (transaction_id, customer_id, amount, timestamp, merchant, geo, geo_lat, geo_lon, is_fraud, dt) 
            VALUES (source.transaction_id, source.customer_id, source.amount, source.timestamp, source.merchant, source.geo, source.geo_lat, source.geo_lon, source.is_fraud, source.dt)
    """)
    record_count = df.count()
    print(f"✅ Transformed & merged {record_count} transactions from bronze to silver at epoch {epoch_id} (time: {datetime.now()})")

# Streaming query: Trigger mỗi 10s để real-time
query = transformed_df.repartition(10).writeStream \
    .foreachBatch(merge_batch) \
    .outputMode("append") \
    .option("checkpointLocation", "/opt/spark/checkpoints/bronze_to_silver_transactions") \
    .trigger(processingTime="10 seconds") \
    .start()

# Chạy liên tục
query.awaitTermination()