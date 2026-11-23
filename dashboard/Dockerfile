# Dashboard nghiệp vụ TrafficSP: FastAPI phục vụ /api/* (đọc Trino, Airflow, Prometheus, Spark) và frontend tĩnh.
# Dùng lại image Python của producer (đã có sẵn trên máy, tránh tải image mới). Code API và bản build web được mount.
FROM ad/producer:3.10
WORKDIR /app/api
COPY api/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
COPY api/ /app/api/
EXPOSE 8095
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8095", "--app-dir", "/app/api"]
