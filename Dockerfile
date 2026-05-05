FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt
COPY archive_proxy.py /app/archive_proxy.py

EXPOSE 8000
CMD ["uvicorn", "archive_proxy:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
