FROM bitnami/spark:3.5.1
USER root

# ================================
# Cài Python + pip + các công cụ build cần thiết
# ================================
RUN apt-get update && apt-get install -y \
    python3-pip python3-dev gcc build-essential curl && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/spark

# ================================
# Cài đặt các thư viện Python cần thiết
# ================================
COPY requirements.txt /opt/spark/requirements.txt
RUN pip3 install --no-cache-dir -r /opt/spark/requirements.txt && rm /opt/spark/requirements.txt

# Add Iceberg runtime compatible with Spark 3.5.1
ENV ICEBERG_VERSION=1.5.0
RUN curl -L -o /opt/bitnami/spark/jars/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar \
        https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar && \
# Add Iceberg core library dependency
    curl -L -o /opt/bitnami/spark/jars/iceberg-aws-bundle-${ICEBERG_VERSION}.jar \
        https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar 
# Add Kafka connector (Spark Structured Streaming)
RUN curl -L -o /opt/bitnami/spark/jars/spark-sql-kafka-0-10_2.12-3.5.1.jar \
        https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar && \
    curl -L -o /opt/bitnami/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.1.jar \
        https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.1/spark-token-provider-kafka-0-10_2.12-3.5.1.jar && \
    curl -L -o /opt/bitnami/spark/jars/kafka-clients-3.5.1.jar \
        https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.1/kafka-clients-3.5.1.jar && \
    curl -L -o /opt/bitnami/spark/jars/commons-pool2-2.11.1.jar \
        https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar

# Add Hadoop AWS support (for S3)
# Make sure the versions match your Hadoop version
# Hadoop 3.3.x requires AWS SDK v1.x
RUN curl -L -o /opt/bitnami/spark/jars/hadoop-aws-3.3.4.jar \
        https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -L -o /opt/bitnami/spark/jars/aws-java-sdk-bundle-1.12.262.jar \
        https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar && \
    curl -L -o /opt/bitnami/spark/jars/aws-sdk-s3-2.25.47.jar \
        https://repo1.maven.org/maven2/software/amazon/awssdk/s3/2.25.47/s3-2.25.47.jar && \
    curl -L -o /opt/bitnami/spark/jars/aws-sdk-sts-2.25.47.jar \
        https://repo1.maven.org/maven2/software/amazon/awssdk/sts/2.25.47/sts-2.25.47.jar

# Add PostgreSQL JDBC driver (for Hive Metastore)
RUN curl -L -o /opt/bitnami/spark/jars/postgresql-42.7.4.jar \
        https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar 
# ================================
# Copy các script từ host vào container
# ================================
COPY ../scripts /opt/spark/scripts
WORKDIR /opt/spark/scripts

CMD [ "tail", "-f", "/dev/null" ]

# Quay lại user spark để chạy Spark
USER 1001