#!/bin/bash
sleep 1

echo "Adding Postgres connection: postgres_default to Airflow for user airflow"
# Add Postgres connection
airflow connections add 'postgres_default' --conn-type 'postgres' --conn-host 'postgres' --conn-login 'airflow' --conn-password 'airflow' --conn-schema 'gharchive' --conn-port 5432

# Add MinIO (S3) connection
echo "Adding MinIO (S3) connection: aws_default to Airflow for user ${MINIO_ROOT_USER}"
airflow connections add 'aws_default' \
  --conn-type 'aws' \
  --conn-login ${MINIO_ROOT_USER} \
  --conn-password ${MINIO_ROOT_PASSWORD} \
  --conn-extra "{\"endpoint_url\": \"http://host.docker.internal:9000\"}"
