FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PLAYWRIGHT_BROWSERS_PATH=/ms-playwright

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY pyproject.toml poetry.lock* ./
RUN pip install "poetry>=1.7" && \
    poetry config virtualenvs.create false && \
    poetry install --no-interaction --no-ansi && \
    playwright install chromium

COPY . .

ENTRYPOINT ["python", "-m", "simplify_downloader"]
