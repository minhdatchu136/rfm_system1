# ══════════════════════════════════════════════════════════════════════
# Dockerfile — RFM Analytics System v2
# Python 3.11, tất cả dependencies trong 1 image
# ══════════════════════════════════════════════════════════════════════

FROM python:3.11-slim

# Tránh prompt tương tác khi cài apt packages
ENV DEBIAN_FRONTEND=noninteractive
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Cài hệ thống dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Cài Python packages (layer riêng để tận dụng cache)
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY . .

# Tạo thư mục data nếu chưa có
RUN mkdir -p /app/data /app/database

# Prefect settings
ENV PREFECT_API_URL=${PREFECT_API_URL:-http://prefect-server:4200/api}

EXPOSE 8501

# Default command (override trong docker-compose)
CMD ["python", "-c", "print('RFM Analytics System v2 ready')"]
