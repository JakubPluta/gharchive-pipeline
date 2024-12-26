#!/bin/bash

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
ENV_FILE="$(pwd)/.env"

echo "Working directory: $(pwd)"
echo "Script directory: $SCRIPT_DIR"
echo "Environment file: $ENV_FILE"

cd "$SCRIPT_DIR"

if [ ! -f "$ENV_FILE" ]; then
    echo "File .env does not exist: $ENV_FILE"
    touch "$ENV_FILE"
    if [ $? -ne 0 ]; then
        echo "Error creating .env in $ENV_FILE."
        exit 1
    fi
fi

if ! grep -q "^AIRFLOW_UID=" "$ENV_FILE"; then
    echo "Adding AIRFLOW_UID to .env"
    echo "AIRFLOW_UID=$(id -u)" >> "$ENV_FILE"
fi

if ! grep -q "^MINIO_ROOT_USER=" "$ENV_FILE"; then
    echo "Adding MINIO_ROOT_USER to .env"
    echo "MINIO_ROOT_USER=USERNAME" >> "$ENV_FILE"
fi

if ! grep -q "^MINIO_ROOT_PASSWORD=" "$ENV_FILE"; then
    echo "Adding MINIO_ROOT_PASSWORD to .env"
    echo "MINIO_ROOT_PASSWORD=PASSWORD" >> "$ENV_FILE"
fi
