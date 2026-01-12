FROM python:3.11-slim

RUN pip install --no-cache-dir poetry

WORKDIR /app

COPY pyproject.toml poetry.lock* README.md ./

COPY src/ ./src/

RUN poetry install --no-interaction --no-ansi

COPY . .

CMD ["poetry", "run", "python3", "-m", "src.runner"]