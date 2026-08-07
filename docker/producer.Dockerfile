FROM python:3.11-slim

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends gcc \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml ./
COPY src/producer ./src/producer

RUN pip install --no-cache-dir \
    "websockets>=13,<16" \
    "confluent-kafka>=2.5" \
    "python-dotenv>=1.0"

ENV PYTHONPATH=/app/src \
    PYTHONUNBUFFERED=1

RUN useradd --create-home --uid 10001 appuser
USER appuser

CMD ["python", "-m", "producer.main"]