build:
# để xây dựng lại các image trong docker-compose
	docker compose build
up:
# để khởi động các container trong docker-compose chế độ nền
	docker compose up -d
down:
	docker compose down
logs:
	docker compose logs -f
ps:
	docker compose ps
restart:
	docker compose restart

load_data:
	python3 load_to_postgres.py