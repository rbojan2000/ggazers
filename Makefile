black:
	black \
		etl/ingestion/src/ \
		etl/ingestion/tests/ \
		etl/transformation/src/ \
		etl/transformation/tests/ \
		etl/load/src/ \
		etl/load/tests/ \
		stream-processing/producer/src/ \
		stream-processing/producer/tests/ \
		stream-processing/connect/

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
		stream-processing/producer/tests/ \
		stream-processing/connect/

etl-tests:
	pytest -v etl/ingestion/tests/ && \
	pytest -v etl/transformation/tests/ && \
	pytest -v etl/load/tests/ && \
	pytest -v stream-processing/producer/tests/

stream-tests:
	cd stream-processing/analyzer && \
	sbt test

check-streams-formatting:
	black --check \
		stream-processing/producer/src/ \
		stream-processing/producer/tests/ \
		stream-processing/connect/ && \
	isort --check --profile black \
		stream-processing/ && \
	flake8 --max-line-length=111 --ignore=E203 \
		stream-processing/producer/src/ \
		stream-processing/producer/tests/ \
		stream-processing/connect/

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

visualization-infrastructure-up:
	docker-compose -f infrastructure/visualization.docker-compose.yml up -d
	
visualization-infrastructure-down:
	docker-compose -f infrastructure/visualization.docker-compose.yml down

streams-infrastructure-up:
	docker-compose -f infrastructure/streams.docker-compose.yml up -d

streams-infrastructure-down:
	docker-compose -f infrastructure/streams.docker-compose.yml down

spark-cluster-up:
	docker-compose -f infrastructure/spark/docker-compose.yml up

spark-cluster-down:
	docker-compose -f infrastructure/spark/docker-compose.yml stop

up: visualization-infrastructure-up streams-infrastructure-up spark-cluster-up
down: visualization-infrastructure-down streams-infrastructure-down spark-cluster-down