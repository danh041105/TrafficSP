# Serving API (FastAPI). Build context: ./serving. Code mount lúc chạy (giống producer), image chỉ chứa thư viện.
# Dùng chung cho chế độ demo (loader/replay ClickHouse).
FROM python:3.10-slim
COPY api/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
WORKDIR /app
ENV PYTHONPATH=/app PYTHONUNBUFFERED=1
EXPOSE 8000
CMD ["uvicorn", "serving.api.main:app", "--host", "0.0.0.0", "--port", "8000"]
