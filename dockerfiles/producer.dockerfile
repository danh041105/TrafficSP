# Producer phát dữ liệu data/raw vào Kafka.
# Code và dữ liệu được mount lúc chạy, image chỉ chứa thư viện.
FROM python:3.10-slim
RUN pip install --no-cache-dir confluent-kafka==2.16.1 pyarrow==25.0.1
WORKDIR /app