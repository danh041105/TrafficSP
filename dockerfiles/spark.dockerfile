# syntax=docker/dockerfile:1
FROM spark:3.5.0-scala2.12-java17-python3-ubuntu

ARG ICEBERG_VERSION=1.10.1
ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_VERSION=1.12.262
ARG POSTGRES_JDBC_VERSION=42.7.4
ARG KAFKA_CLIENTS_VERSION=3.4.1
ARG COMMONS_POOL2_VERSION=2.11.1

USER root

# Iceberg cho Spark 3.5 (Scala 2.12) + S3FileIO để ghi bảng Iceberg lên MinIO
ADD --chmod=644 https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar /opt/spark/jars/
ADD --chmod=644 https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar /opt/spark/jars/

# s3a:// để đọc file KPI thô (CSV) trên MinIO - phải khớp Hadoop 3.3.4 có sẵn trong Spark 3.5.0
ADD --chmod=644 https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar /opt/spark/jars/
ADD --chmod=644 https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar /opt/spark/jars/

# Driver JDBC cho Iceberg JDBC Catalog trên PostgreSQL
ADD --chmod=644 https://repo1.maven.org/maven2/org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar /opt/spark/jars/

# Kafka - Spark Structured Streaming
ADD --chmod=644 https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar /opt/spark/jars/
ADD --chmod=644 https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.0/spark-token-provider-kafka-0-10_2.12-3.5.0.jar /opt/spark/jars/
ADD --chmod=644 https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/${KAFKA_CLIENTS_VERSION}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar /opt/spark/jars/
ADD --chmod=644 https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/${COMMONS_POOL2_VERSION}/commons-pool2-${COMMONS_POOL2_VERSION}.jar /opt/spark/jars/

# Speed layer: driver ghi ClickHouse qua HTTP trong foreachBatch, cần pandas cho toPandas()
RUN pip install --no-cache-dir "clickhouse-connect>=0.8,<0.9" "pandas==2.2.3" "pyarrow==17.0.0"

RUN mkdir -p /opt/spark/jobs && chown -R spark:spark /opt/spark/jobs

USER spark
WORKDIR /opt/spark
