.PHONY: help up down logs test test-fast test-dag test-fx test-data-quality test-weather

# General
help:
	@echo "Available targets:"
	@echo "  make up                - Start all services with build"
	@echo "  make down              - Stop all services"
	@echo "  make logs              - Follow docker compose logs"
	@echo "  make test              - Run all tests"
	@echo "  make test-fast         - Run fast local test subset"
	@echo "  make test-dag          - Run DAG loading tests"
	@echo "  make test-fx           - Run FX pipeline tests"
	@echo "  make test-data-quality - Run data quality tests"
	@echo "  make test-weather      - Run weather pipeline tests"

# Docker
up:
	docker compose up -d --build

down:
	docker compose down

logs:
	docker compose logs -f

# Tests
test:
	docker exec -e PYTHONPATH=/opt/airflow/dags airflow-webserver pytest tests/

test-fast:
	docker exec -e PYTHONPATH=/opt/airflow/dags airflow-webserver pytest tests/test_data_quality.py tests/test_fx_pipeline.py tests/test_weather_pipeline.py

test-dag:
	docker exec -e PYTHONPATH=/opt/airflow/dags airflow-webserver pytest tests/test_fx_pipeline.py tests/test_weather_pipeline.py

test-fx:
	docker exec -e PYTHONPATH=/opt/airflow/dags airflow-webserver pytest tests/test_fx_pipeline.py

test-data-quality:
	docker exec -e PYTHONPATH=/opt/airflow/dags airflow-webserver pytest tests/test_data_quality.py

test-weather:
	docker exec -e PYTHONPATH=/opt/airflow/dags airflow-webserver pytest tests/test_weather_pipeline.py
