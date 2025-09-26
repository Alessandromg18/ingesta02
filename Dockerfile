FROM python:3.10-slim

RUN apt-get update && apt-get install -y \
    gcc \
    default-libmysqlclient-dev \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /programas/ingesta

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY export_to_s3.py .

CMD ["python", "export_to_s3.py"]
