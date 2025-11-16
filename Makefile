black:
	black etl/ingestion/src/ etl/ingestion/tests/

isort:
	isort etl/

flake:
	flake8 --max-line-length=100 --ignore=E203 etl/ingestion/src/ etl/ingestion/tests/

tests:
	pytest -v etl/ingestion/tests/

run-ingestion:
	cd etl/ingestion && \
	python3 -m src.runner