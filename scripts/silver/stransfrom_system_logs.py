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
    .load("iceberg_catalog.bronze.finance.system_logs") \
    .withWatermark("timestamp", "1 hour")  # Handle late data từ Kafka
# Transform real-time: Clean, enrich
transformed_df = bronze_df \
    .filter(col("log_id").isNotNull() & col("message").isNotNull()) \
    .withColumn("timestamp_clean", to_timestamp(col("timestamp"))) \
    .withColumn("dt", to_date(col("timestamp_clean"))) \
    .withColumn("service_clean", when(col("service").isin("api", "database", "auth", "payment", "notification", "analytics", "storage", "search", "caching", "load_balancer"), col("service")).otherwise("unknown")) \
    .withColumn("component", when(col("component").isin("frontend", "backend", "database", "cache", "message_queue"), col("component")).otherwise("unknown")) \
    .withColumn("log_level", when(col("severity") == "ERROR", 4)
                          .when(col("severity") == "WARN", 3)
                          .when(col("severity") == "INFO", 2)
                          .otherwise(1)) \
    .dropDuplicates(["log_id", "dt"]) \
    .select(
        col("log_id"),
        col("timestamp_clean").alias("timestamp"),
        col("service_clean").alias("service"),
        col("component"),
        col("severity"),
        col("log_level"),
        col("message"),
        col("dt")
    )
# Function MERGE batch mới từ bronze vào silver
def merge_batch(df, epoch_id):
    if df.rdd.isEmpty():
        print(f"⚠️ Empty batch at epoch {epoch_id}")
        return
    df.createOrReplaceTempView("bronze_batch")
    spark.sql("""
        MERGE INTO iceberg_catalog.silver.finance.system_logs AS target
        USING bronze_batch AS source
        ON target.log_id = source.log_id AND target.dt = source.dt
        WHEN MATCHED THEN
            UPDATE SET
                timestamp = source.timestamp,
                service = source.service,
                component = source.component,
                severity = source.severity,
                log_level = source.log_level,
                message = source.message
        WHEN NOT MATCHED THEN
            INSERT (log_id, timestamp, service, component, severity, log_level, message, dt)
            VALUES (source.log_id, source.timestamp, source.service, source.component, source.severity, source.log_level, source.message, source.dt)
    """)
    print(f"✅ Batch {epoch_id} merged at {datetime.now()} with {df.count()} records")

# Streaming query: Trigger mỗi 10s để real-time
query = transformed_df.repartition(10).writeStream \
    .outputMode("append") \
    .foreachBatch(merge_batch) \
    .trigger(processingTime="10 seconds") \
    .start()

query.awaitTermination()