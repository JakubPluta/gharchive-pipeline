import gzip
import io
import json
import sys
from logging import getLogger

import duckdb
import pendulum
import requests
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow import DAG
from airflow.decorators import task, dag
from airflow.utils.dates import days_ago
from gha.duckdb_repo import DuckDBConfiguration, DuckDBRepository, get_default_duckdb_client_from_env
from airflow.operators.dummy import DummyOperator
from airflow.providers.amazon.aws.transfers.s3_to_sql import S3ToSqlOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator


def parse_parquet_generator(filepath):
    import pyarrow as pa
    import pyarrow.parquet as pq
    table = pq.read_table(filepath)
    chunk_size = 1024
    for batch in table.to_batches(chunk_size):
        for row in batch.to_pylist():
            yield row.values()




logger = getLogger(__name__)

AWS_CONN_ID = "aws_default"
S3_BUCKET = "datalake"
S3_KEY = "gharchive/raw"
SQL_CONN_ID = "postgres"

BASE_URL = "http://data.gharchive.org"


def _get_github_archive(date: pendulum.DateTime) -> bytes | None:
    """
    Get a github archive from a given date.

    The file is retrieved from the gharchive.org API and the response body is
    returned as bytes.

    Args:
        date: The date of the archive to retrieve.

    Returns:
        The response body as bytes if successful, None if the request failed.
    """
    filename = f'{date.strftime("%Y-%m-%d-%-H")}.json.gz'
    url = f"{BASE_URL}/{filename}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.content
    except requests.exceptions.RequestException as e:
        logger.error("could not get %s: %s", url, e)
        return None


def _get_s3_partition_key(base_key: str, logical_date: pendulum.DateTime) -> str:
    """
    Generates the S3 key based on the logical date.
    """
    date_partition = logical_date.strftime("%Y-%m-%d")
    hour_partition = logical_date.strftime("%H")
    return f"{base_key}/{date_partition}/{hour_partition}/events.json.gz"


@dag(
    start_date=days_ago(1),
    schedule_interval="@hourly",
    catchup=True,
    tags=["github"],
    max_active_runs=1,
    concurrency=1,
)
def github_archive_pipeline():
    @task
    def create_s3_bucket(s3_bucket: str) -> str:
        """
        Create an S3 bucket if it does not exist.

        Args:
            s3_bucket: The name of the S3 bucket to create.

        Returns:
            The name of the S3 bucket created.
        """
        s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
        logger.info("checking if bucket %s exists", s3_bucket)
        if not s3_hook.check_for_bucket(bucket_name=s3_bucket):
            logger.info("bucket does not exist, creating bucket %s", s3_bucket)
            s3_hook.create_bucket(bucket_name=s3_bucket)
        else:
            logger.info("bucket %s exists", s3_bucket)

        return s3_bucket

    @task
    def github_archive_to_s3(s3_bucket: str, s3_key: str, **context) -> str:
        """
        Task to fetch GitHub archive data and store it in an S3 bucket.

        Args:
            s3_bucket: Target S3 bucket name.
            s3_key: Base key for S3 object storage.
            context: Airflow context dictionary.

        Returns:
            The S3 key where the data was stored.
        """
        s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
        logical_date: pendulum.Datetime = context["logical_date"]
        s3_key = _get_s3_partition_key(s3_key, logical_date)

        logger.info("writing to s3 %s %s", s3_bucket, s3_key)
        try:
            github_data = _get_github_archive(logical_date)
            logger.info("fetched %s bytes of data", sys.getsizeof(github_data))
            # check size of data
            if sys.getsizeof(github_data) < 1:
                logger.info("no data found for %s", logical_date)
                return s3_key

            s3_hook.load_bytes(
                bytes_data=github_data,
                key=s3_key,
                bucket_name=s3_bucket,
                replace=True,
            )
        except Exception as e:
            logger.error("could not write to s3: %s", e)
            raise e
        logger.info("successfully wrote to s3 %s %s", s3_bucket, s3_key)
        return s3_key

    @task
    def clean_raw_data(s3_bucket, s3_key, **context):
        """
        Task to clean raw data stored in S3, transform it, and save as Parquet.

        Args:
            s3_bucket: S3 bucket name containing raw data.
            s3_key: S3 key for raw data. It's full key to object filename:
                eg: raw/2022-01-01/00/events.json.gz
            context: Airflow context dictionary.

        Returns:
            Destination S3 path for the cleaned data.
        """
        logger.debug("cleaning raw data for %s %s", s3_bucket, s3_key)
        raw_table_name, clean_table_name = "raw", "clean"

        duck_client = get_default_duckdb_client_from_env(is_container=True)
        source_path = f"s3://{s3_bucket}/{s3_key}"
        query = """
            SELECT
            id as event_id,
            type as event_type,
            created_at as event_created_at,
            public as is_public,
            actor.id as user_id,
            actor.login as user_login,
            actor.display_login as user_display_login,
            repo.id as repo_id,
            repo.name as repo_name,
            repo.url as repo_url
            FROM raw
        """

        destination_path = source_path.replace("raw", "clean")
        destination_path = destination_path.replace(".json.gz", ".parquet")

        logger.info("preparing destination path for transformed data from source: %s to %s", source_path,
                    destination_path)

        with duck_client.connection_context() as client:
            client.execute(
                f"CREATE OR REPLACE TABLE {raw_table_name} AS FROM read_json_auto('{source_path}', ignore_errors=true)")
            client.execute(f"CREATE OR REPLACE TABLE {clean_table_name} AS FROM ({query})")
            table = client.conn.table(clean_table_name)
            logger.info("writing cleaned parquet data to: %s", destination_path)
            table.write_parquet(destination_path)
        file_key = destination_path.split(s3_bucket)[1]
        return file_key

    create_table = PostgresOperator(
        task_id="create_table",
        postgres_conn_id=SQL_CONN_ID,
        sql="""
            CREATE TABLE IF NOT EXISTS github_archive (
                event_id VARCHAR(255),
                event_type VARCHAR(255),
                event_created_at TIMESTAMP,
                is_public BOOLEAN,
                user_id VARCHAR(255),
                user_login VARCHAR(255),
                user_display_login VARCHAR(255),
                repo_id VARCHAR(255),
                repo_name VARCHAR(255),
                repo_url VARCHAR(255)
            );
        """,
    )

    transfer_s3_to_sql_generator = S3ToSqlOperator(
        task_id="transfer_s3_to_sql_paser_to_generator",
        s3_bucket=S3_BUCKET,
        aws_conn_id=AWS_CONN_ID,
        table="github_archive",
        parser=parse_parquet_generator,
        sql_conn_id=SQL_CONN_ID,
        s3_key='{{ task_instance.xcom_pull(task_ids="clean_raw_data") }}',

    )

    start = DummyOperator(task_id="start")
    create_s3_bucket_task = create_s3_bucket(S3_BUCKET)
    load_gh_archive_task = github_archive_to_s3(s3_bucket=S3_BUCKET, s3_key=S3_KEY)
    clean_raw_data_task = clean_raw_data(s3_bucket=S3_BUCKET, s3_key=load_gh_archive_task)

    end = DummyOperator(task_id="end")

    start >> [create_s3_bucket_task,
              create_table] >> load_gh_archive_task >> clean_raw_data_task >> transfer_s3_to_sql_generator >> end


github_archive_pipeline()
