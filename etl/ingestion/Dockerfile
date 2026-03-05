FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    POETRY_VERSION=2.3.1 \
    POETRY_NO_INTERACTION=1 \
    POETRY_VIRTUALENVS_CREATE=false \
    PIP_NO_CACHE_DIR=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl build-essential \
    && rm -rf /var/lib/apt/lists/*

RUN pip install "poetry==$POETRY_VERSION"

WORKDIR /app

COPY pyproject.toml poetry.lock README.md ./
COPY src ./src

RUN POETRY_VIRTUALENVS_CREATE=false \
    poetry install --only main --no-root --no-interaction --no-ansi

ENV PYTHONPATH=/app/src

CMD ["poetry", "run", "python3", "-m", "src.runner"]
