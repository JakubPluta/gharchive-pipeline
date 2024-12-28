

prepdir:
	@echo "Creating directories for airflow: ./dags, ./logs, ./plugins, ./config, ./tests ./include (if not exist)"
	mkdir -p ./dags ./logs ./plugins ./tests ./config ./include
	@echo "creating default env file"
	./init-env.sh

local-install:
	@echo "Installing dependencies locally"
	pip install --upgrade pip
	pip install -r requirements.txt
	@echo "Dependencies installed"

build: prepdir
	@echo "Building Docker images"
	docker compose up -d --build

up: prepdir
	@echo "Starting Docker containers"
	docker compose up -d

down:
	@echo "Stopping Docker containers"
	docker compose down

recreate: prepdir
	@echo "Recreating Docker containers"
	docker compose up -d --force-recreate

cli:
	@echo "Starting Airflow CLI"
	@echo "fetching container name"
	@CONTAINER_ID=$$(docker ps --filter "name=webserver" --format "{{.ID}}"); \
	echo "Starting container CLI for $$CONTAINER_ID"; \
	docker exec -it $$CONTAINER_ID /bin/bash
