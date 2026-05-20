FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DEFAULT_TIMEOUT=120 \
    PIP_RETRIES=10 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

COPY pyproject.toml ./
COPY src/ ./src/
RUN pip install --upgrade pip && pip install .

RUN mkdir -p /data
VOLUME ["/data"]

ENTRYPOINT ["python", "-m", "cloud_transferer.cli"]
CMD ["run"]
