up:
	docker compose up -d --build

down:
	docker compose down

logs:
	docker compose logs -f

test:
	docker exec airflow-webserver pytest tests/
