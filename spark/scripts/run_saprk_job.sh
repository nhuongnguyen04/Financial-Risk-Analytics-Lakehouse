#!/bin/bash

# run_spark_job.sh

# Đường dẫn đến script Python
SCRIPT_PATH="/opt/spark/scripts/ingest_kafka_to_bronze.py"

# Kiểm tra xem script có tồn tại không
if [ ! -f "$SCRIPT_PATH" ]; then
    echo "Lỗi: File $SCRIPT_PATH không tồn tại trong container"
    exit 1
fi

# Chạy spark-submit
echo "Bắt đầu chạy Spark job..."
spark-submit \
    --master spark://spark:7077 \
    --conf "spark.driver.memory=4g" \
    --conf "spark.executor.memory=4g" \
    --conf "spark.hadoop.fs.s3a.endpoint=http://minio:9000" \
    --conf "spark.hadoop.fs.s3a.access.key=minioadmin" \
    --conf "spark.hadoop.fs.s3a.secret.key=12345678" \
    --conf "spark.hadoop.fs.s3a.path.style.access=true" \
    --conf "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem" \
    $SCRIPT_PATH

# Kiểm tra trạng thái
if [ $? -eq 0 ]; then
    echo "Spark job chạy thành công!"
else
    echo "Lỗi khi chạy Spark job, kiểm tra log để biết chi tiết."
    exit 1
fi