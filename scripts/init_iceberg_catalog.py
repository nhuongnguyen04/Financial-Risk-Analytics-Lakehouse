from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Init Iceberg Catalog") \
    .config("spark.sql.catalog.iceberg_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.iceberg_catalog.type", "jdbc") \
    .config("spark.sql.catalog.iceberg_catalog.uri", "jdbc:postgresql://postgres:5432/iceberg_catalog") \
    .config("spark.sql.catalog.iceberg_catalog.jdbc.user", "iceberg") \
    .config("spark.sql.catalog.iceberg_catalog.jdbc.password", "iceberg") \
    .config("spark.sql.catalog.iceberg_catalog.warehouse", "s3a://lake/") \
    .config("spark.sql.catalog.iceberg_catalog.jdbc.schema-version", "V1") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.sql.catalog.iceberg_catalog.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.sql.catalog.iceberg_catalog.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.sql.catalog.iceberg_catalog.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.sql.catalog.iceberg_catalog.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

# Tạo namespace nếu chưa tồn tại
spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg_catalog.finance")
spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg_catalog.system")
spark.stop()
print("✅ Iceberg catalog & example table initialized")