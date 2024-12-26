

prepdir:
	@echo "Creating directories for airflow: ./dags, ./logs, ./plugins, ./config, ./tests (if not exist)"
	mkdir -p ./dags ./logs ./plugins ./tests ./config
	@echo "creating default env file"
	./scripts/init_dotenv.sh

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
	MINIO_ROOT_LOGIN=$$(grep 'MINIO_ROOT_LOGIN' .env | cut -d '=' -f2); \
	MINIO_ROOT_PASSWORD=$$(grep 'MINIO_ROOT_PASSWORD' .env | cut -d '=' -f2); \
	docker exec $$CONTAINER_ID /bin/bash -c "airflow connections add 's3' --conn-type 'aws' --conn-login $$MINIO_ROOT_LOGIN --conn-password $$MINIO_ROOT_PASSWORD --conn-extra '{\"endpoint_url\": \"http://host.docker.internal:9000\"}'"

