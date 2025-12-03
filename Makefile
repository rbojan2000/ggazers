black:
	black \
		etl/ingestion/src/ \
		etl/ingestion/tests/ \
		etl/transformation/src/ \
		etl/transformation/tests/

isort:
	isort --profile black etl/

flake:
	flake8 --max-line-length=100 --ignore=E203 \
		etl/ingestion/src/ \
		etl/ingestion/tests/ \
		etl/transformation/src/ \
		etl/transformation/tests/

tests:
	pytest -v etl/ingestion/tests/ && \
	pytest -v etl/transformation/tests/

check-etl-formatting:
	black --check \
		etl/ingestion/src/ \
		etl/ingestion/tests/ \
		etl/transformation/src/ \
		etl/transformation/tests/ && \
	isort --check --profile black \
		etl/ && \
	flake8 --max-line-length=100 --ignore=E203 \
		etl/ingestion/src/ \
		etl/ingestion/tests/ \
		etl/transformation/src/ \
		etl/transformation/tests/

run-ingestion:
	cd etl/ingestion && \
	python3 -m src.runner