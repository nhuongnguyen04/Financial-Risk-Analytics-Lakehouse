from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, to_date
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, DateType

# Cấu hình Spark với Iceberg và Kafka
spark = SparkSession.builder \
    .appName('stream_to_iceberg') \
    .config('spark.sql.streaming.schemaInference', 'true') \
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

# Cấu hình Kafka
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
    StructField("action", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("ip_address", StringType(), True)
])
system_log_schema = StructType([
    StructField("log_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("service", StringType(), True),
    StructField("severity", StringType(), True),
    StructField("message", StringType(), True)
])

# Hàm xử lý và ghi dữ liệu vào Iceberg
def process_and_write_to_iceberg(topic, schema, table):
    topic_df = kafka_df.filter(col("topic") == topic) \
        .selectExpr("CAST(value AS STRING) as json_str") \
        .select(from_json(col("json_str"), schema).alias("data")) \
        .select("data.*")

    topic_df.writeStream \
        .format("iceberg") \
        .option("checkpointLocation", f"/tmp/checkpoints/{topic}") \
        .table(table) \
        .start()
    print(f"Started streaming for topic {topic} to table {table}")
    return topic_df
# Xử lý từng topic
transaction_df = process_and_write_to_iceberg("transactions", transaction_schema, "iceberg_catalog.finance.transactions")
auth_log_df = process_and_write_to_iceberg("auth_logs", auth_log_schema, "iceberg_catalog.system.auth_logs")
system_log_df = process_and_write_to_iceberg("system_logs", system_log_schema, "iceberg_catalog.system.system_logs")
# Giữ ứng dụng chạy
spark.streams.awaitAnyTermination()