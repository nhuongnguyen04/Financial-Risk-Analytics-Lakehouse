from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date

# Khởi tạo Spark
spark = SparkSession.builder \
    .appName("KafkaToBronze") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "12345678") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# Cấu hình Kafka
kafka_bootstrap_servers = "kafka:9092"
topics = ["transactions", "auth_logs", "system_logs"]

# Đọc từ Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", ",".join(topics)) \
    .option("startingOffsets", "earliest") \
    .load()

# Ghi vào MinIO
for topic in topics:
    topic_df = df.filter(col("topic") == topic) \
                 .selectExpr("CAST(value AS STRING) as value", "timestamp") \
                 .withColumn("dt", to_date(col("timestamp")))
    
    # Xác định đường dẫn lưu trữ dựa trên topic
    if topic in ["auth_logs", "system_logs"]:
        path = f"s3a://lake/bronze/system/{topic}/dt={datetime.now().strftime('%Y-%m-%d')}"
    else:
        path = f"s3a://lake/bronze/finance/{topic}/dt={datetime.now().strftime('%Y-%m-%d')}"
    
    query = topic_df.writeStream \
        .format("parquet") \
        .option("path", path) \
        .option("checkpointLocation", f"s3a://lake/checkpoints/{topic}") \
        .partitionBy("dt") \
        .trigger(processingTime="10 seconds") \
        .start()

# Giữ job streaming chạy
spark.streams.awaitAnyTermination()