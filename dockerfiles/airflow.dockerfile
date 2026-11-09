# syntax=docker/dockerfile:1
# Airflow 3.3.2 trên Python 3.10 -> khớp Python 3.10 của các Spark worker
FROM apache/airflow:slim-3.3.2-python3.10

ARG PYSPARK_VERSION=3.5.0
ARG ICEBERG_VERSION=1.10.1
ARG GX_VERSION=1.24.1
ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_VERSION=1.12.262
ARG POSTGRES_JDBC_VERSION=42.7.4
ARG KAFKA_CLIENTS_VERSION=3.4.1
ARG COMMONS_POOL2_VERSION=2.11.1


# Java 17: spark-submit (driver) cần JVM
USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
 && apt-get autoremove -yqq --purge \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

USER airflow

RUN pip install --no-cache-dir \
    "apache-airflow[postgres]==${AIRFLOW_VERSION}" \
    apache-airflow-providers-fab \
    --constraint "${HOME}/constraints.txt"

# PySpark cùng phiên bản với cụm Spark -> có sẵn lệnh spark-submit
# Giữ nguyên phiên bản Airflow để pip không tự nâng/hạ Airflow
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" "pyspark==${PYSPARK_VERSION}"

# Driver chạy trong container Airflow nên cũng cần đủ JAR như image Spark
RUN JARS_DIR=$(python -c "import os, pyspark; print(os.path.join(os.path.dirname(pyspark.__file__), 'jars'))") \
 && cd "$JARS_DIR" \
 && curl -fsSLO https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar \
 && curl -fsSLO https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar \
 && curl -fsSLO https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar \
 && curl -fsSLO https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar \
 && curl -fsSLO https://repo1.maven.org/maven2/org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar \
 && curl -fsSLO https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/${PYSPARK_VERSION}/spark-sql-kafka-0-10_2.12-${PYSPARK_VERSION}.jar \
 && curl -fsSLO https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/${PYSPARK_VERSION}/spark-token-provider-kafka-0-10_2.12-${PYSPARK_VERSION}.jar \
 && curl -fsSLO https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/${KAFKA_CLIENTS_VERSION}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar \
 && curl -fsSLO https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/${COMMONS_POOL2_VERSION}/commons-pool2-${COMMONS_POOL2_VERSION}.jar

# Great Expectations
RUN python -m pip install --no-cache-dir "great_expectations==${GX_VERSION}" \
 && python -m pip check