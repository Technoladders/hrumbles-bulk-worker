FROM python:3.11-slim

LABEL maintainer="hrumbles"
LABEL description="Bulk resume upload pipeline worker"

WORKDIR /app

# System dependencies for PDF parsing (pdfminer, pypdf — no OCR needed)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpoppler-cpp-dev \
    poppler-utils \
    antiword \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Application code
COPY . .

RUN chmod +x entrypoint.sh

EXPOSE 5010

HEALTHCHECK --interval=30s --timeout=10s --start-period=15s --retries=3 \
  CMD curl -f http://localhost:5010/health || exit 1

CMD ["./entrypoint.sh"]