black:
	black \
		etl/ingestion/src/ \
		etl/ingestion/tests/ \
		etl/transformation/src/ \
		etl/transformation/tests/ \
		etl/load/src/ \
		etl/load/tests/ \
		stream-processing/producer/src/ \
		stream-processing/producer/tests/

isort:
	isort --profile black etl/ stream-processing/

flake:
	flake8 --max-line-length=111 --ignore=E203 \
		etl/ingestion/src/ \
		etl/ingestion/tests/ \
		etl/transformation/src/ \
		etl/transformation/tests/ \
		etl/load/src/ \
		etl/load/tests/ \
		stream-processing/producer/src/ \
		stream-processing/producer/tests/

tests:
	pytest -v etl/ingestion/tests/ && \
	pytest -v etl/transformation/tests/ && \
	pytest -v etl/load/tests/ && \
	pytest -v stream-processing/producer/tests/

check-streams-formatting:
	black --check \
		stream-processing/producer/src/ \
		stream-processing/producer/tests/ && \
	isort --check --profile black \
		stream-processing/ && \
	flake8 --max-line-length=111 --ignore=E203 \
		stream-processing/producer/src/ \
		stream-processing/producer/tests/

check-etl-formatting:
	black --check \
		etl/ingestion/src/ \
		etl/ingestion/tests/ \
		etl/transformation/src/ \
		etl/transformation/tests/ && \
	isort --check --profile black \
		etl/ && \
	flake8 --max-line-length=111 --ignore=E203 \
		etl/ingestion/src/ \
		etl/ingestion/tests/ \
		etl/transformation/src/ \
		etl/transformation/tests/ \
		etl/load/src/ \
		etl/load/tests/

check-formatting: check-streams-formatting check-etl-formatting

streams-infrastructure-up:
	docker-compose -f infrastructure/streams.docker-compose.yml up

streams-infrastructure-down:
	docker-compose -f infrastructure/streams.docker-compose.yml down