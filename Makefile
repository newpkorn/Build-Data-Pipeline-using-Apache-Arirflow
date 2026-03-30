.PHONY: help up down logs test test-fast test-dag test-fx test-data-quality test-weather migrate-weather-001 migrate-weather-002 verify-weather-002

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
	@echo "  make migrate-weather-001 - Run weather region migration (001)"
	@echo "  make migrate-weather-002 - Run weather daily uniqueness migration (002)"
	@echo "  make verify-weather-002  - Verify weather daily uniqueness constraints"

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

# Weather migrations
migrate-weather-001:
	docker compose up -d mysql
	docker compose exec -T mysql sh -lc 'mysql -u"$$MYSQL_USER" -p"$$MYSQL_PASSWORD" "$${MYSQL_DATABASE:-$$MYSQL_DB}"' < docker/mysql/migrations/001_weather_observations_region_upgrade.sql

migrate-weather-002:
	docker compose up -d mysql
	docker compose exec -T mysql sh -lc 'mysql -u"$$MYSQL_USER" -p"$$MYSQL_PASSWORD" "$${MYSQL_DATABASE:-$$MYSQL_DB}"' < docker/mysql/migrations/002_weather_observations_daily_uniqueness.sql

verify-weather-002:
	docker compose exec -T mysql sh -lc 'mysql -u"$$MYSQL_USER" -p"$$MYSQL_PASSWORD" "$${MYSQL_DATABASE:-$$MYSQL_DB}" -e "SHOW INDEX FROM weather_observations; SELECT COUNT(*) AS total_rows, COUNT(DISTINCT CONCAT(province, '\''|'\'', observed_date)) AS distinct_province_day FROM weather_observations; SELECT province, observed_date, COUNT(*) AS cnt FROM weather_observations GROUP BY province, observed_date HAVING COUNT(*) > 1 LIMIT 20;"'
