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
		stream-processing/connect/ \
		dags/

isort:
	isort --profile black etl/ stream-processing/ dags/

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
		stream-processing/connect/ \
		dags/

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
		etl/transformation/tests/ \
		dags/ && \
	isort --check --profile black \
		etl/ dags/ && \
	flake8 --max-line-length=111 --ignore=E203 \
		etl/ingestion/src/ \
		etl/ingestion/tests/ \
		etl/transformation/src/ \
		etl/transformation/tests/ \
		etl/load/src/ \
		etl/load/tests/ \
		dags/


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
	docker-compose -f infrastructure/spark/docker-compose.yml up -d

spark-cluster-down:
	docker-compose -f infrastructure/spark/docker-compose.yml stop

airflow-up:
	docker-compose -f infrastructure/airflow/docker-compose.yml up -d

airflow-down:
	docker-compose -f infrastructure/airflow/docker-compose.yml down

up: visualization-infrastructure-up streams-infrastructure-up spark-cluster-up airflow-up
down: visualization-infrastructure-down streams-infrastructure-down spark-cluster-down airflow-down