

prepdir:
	@echo "Creating directories for airflow: ./dags, ./logs, ./plugins, ./config, ./tests (if not exist)"
	mkdir -p ./dags ./logs ./plugins ./tests ./config
	@echo "Pushing AIRFLOW_UID to .env file"
	if ! grep -q "AIRFLOW_UID" .env; then echo "AIRFLOW_UID=$(shell id -u)" >> .env; fi

local-install:
	@echo "Installing dependencies locally"
	pip install --upgrade pip
	pip install -r requirements.txt
	@echo "Dependencies installed"

build: prepdir
	@echo "Building Docker images"
	docker compose up -d --build

up:
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


s3-conn:
	@echo "Adding S3 connection to Airflow"
	@CONTAINER_ID=$$(docker ps --filter "name=webserver" --format "{{.ID}}"); \
	docker exec $$CONTAINER_ID /bin/bash -c "airflow connections add 's3' --conn-type 'aws' --conn-login 'USERNAME' --conn-password 'PASSWORD' --conn-extra '{\"endpoint_url\": \"http://host.docker.internal:9000\"}'"

