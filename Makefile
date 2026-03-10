up:
	docker compose up -d --build

down:
	docker compose down

logs:
	docker compose logs -f

test:
	docker exec -e PYTHONPATH=/opt/airflow/dags airflow-webserver pytest tests/
