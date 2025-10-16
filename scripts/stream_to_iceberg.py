from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, to_date, when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, DateType

# Cấu hình Spark với Iceberg và Kafka
spark = SparkSession.builder \
    .appName('stream_to_iceberg') \
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



# # Cấu hình Kafka
kafka_bootstrap_servers = "kafka:9092"
topics = ["transactions", "auth_logs", "system_logs"]
kafka_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", ",".join(topics)) \
    .option("startingOffsets", "earliest") \
    .load()

# Định nghĩa schema cho từng topic
transaction_schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("amount", DoubleType(), True),
    StructField("timestamp", StringType(), True),
    StructField("merchant", StringType(), True),
    StructField("geo", StringType(), True),
    StructField("is_fraud", IntegerType(), True)
])

auth_log_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("timestamp", StringType(), True),
    StructField("action", StringType(), True),
    StructField("device_id", StringType(), True),
    StructField("ip_address", StringType(), True),
    StructField("geo", StringType(), True)
])

system_log_schema = StructType([
    StructField("log_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("service", StringType(), True),
    StructField("severity", StringType(), True),
    StructField("message", StringType(), True)
])

# Parse JSON with multiple schemas; mismatched will be null
parsed_df = kafka_df \
    .selectExpr("CAST(value AS STRING) as json_value", "topic") \
    .select(
        from_json(col("json_value"), transaction_schema).alias("transaction_data"),
        from_json(col("json_value"), auth_log_schema).alias("auth_log_data"),
        from_json(col("json_value"), system_log_schema).alias("system_log_data"),
        col("topic")
    )

# Tách DataFrame theo từng topic, selecting non-null data
transactions_df = parsed_df \
    .filter(col("topic") == "transactions") \
    .filter(col("transaction_data").isNotNull()) \
    .select("transaction_data.*")

auth_logs_df = parsed_df \
    .filter(col("topic") == "auth_logs") \
    .filter(col("auth_log_data").isNotNull()) \
    .select("auth_log_data.*")

system_logs_df = parsed_df \
    .filter(col("topic") == "system_logs") \
    .filter(col("system_log_data").isNotNull()) \
    .select("system_log_data.*")

# Write to Iceberg function
def write_to_iceberg(df, epoch_id, namespace, table_name):
    try:
        df = df.withColumn("timestamp", to_timestamp(col("timestamp"))) \
               .withColumn("dt", to_date(col("timestamp")))
        df.writeTo(f"iceberg_catalog.{namespace}.{table_name}").append()
        print(f"✅ Ghi thành công vào iceberg_catalog.{namespace}.{table_name}, số bản ghi: {df.count()}")
    except Exception as e:
        df.writeTo(f"iceberg_catalog.{namespace}.{table_name}").using("iceberg").createOrReplace()
        df.writeTo(f"iceberg_catalog.{namespace}.{table_name}").append()
        print(f"✅ Tạo mới bảng iceberg_catalog.{namespace}.{table_name} và ghi dữ liệu, số bản ghi: {df.count()}")

# Thiết lập streaming query song song cho từng DataFrame
query_transactions = transactions_df.writeStream \
    .foreachBatch(lambda df, epoch_id: write_to_iceberg(df, epoch_id, "finance", "transactions")) \
    .outputMode("append") \
    .start()

query_auth_logs = auth_logs_df.writeStream \
    .foreachBatch(lambda df, epoch_id: write_to_iceberg(df, epoch_id, "system", "auth_logs")) \
    .outputMode("append") \
    .start()

query_system_logs = system_logs_df.writeStream \
    .foreachBatch(lambda df, epoch_id: write_to_iceberg(df, epoch_id, "system", "system_logs")) \
    .outputMode("append") \
    .start()

# Chờ các query kết thúc
query_transactions.awaitTermination()
query_auth_logs.awaitTermination()
query_system_logs.awaitTermination()