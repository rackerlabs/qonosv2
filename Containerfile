FROM python:3.12-slim-bookworm

LABEL org.opencontainers.image.name="qonosv2"
LABEL org.opencontainers.image.description="QonoS v2 scheduling service for OpenStack."

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

WORKDIR /app

COPY pyproject.toml README.md alembic.ini ./
COPY api /app/api
COPY db /app/db

RUN pip install --no-cache-dir .

COPY spec /app/spec

RUN mkdir -p /etc/qonosv2 \
  && chown -R 65534:65534 /app /etc/qonosv2

USER 65534:65534

ENTRYPOINT ["uvicorn", "api.app.main:app", "--host", "0.0.0.0", "--port", "8080"]
