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

logger = getLogger(__name__)


AWS_CONN_ID = "aws_default"
S3_BUCKET = "datalake"
S3_KEY = "gharchive/raw"

BASE_URL = "http://data.gharchive.org"


def _get_github_archive(date: pendulum.DateTime) -> bytes | None:
    filename = f'{date.strftime("%Y-%m-%d-%-H")}.json.gz'
    url = f"{BASE_URL}/{filename}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.content
    except requests.exceptions.RequestException as e:
        logger.error("could not get %s: %s", url, e)
        return None

@dag(
    start_date=days_ago(n=0, hour=6),
    schedule_interval="@hourly",
    catchup=True,
    tags=["github"],
)
def github_archive_pipeline():

    @task
    def create_s3_bucket(s3_bucket):
        s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
        logger.info("checking if bucket %s exists", s3_bucket)
        if not s3_hook.check_for_bucket(bucket_name=s3_bucket):
            logger.info("bucket does not exist, creating bucket %s", s3_bucket)
            s3_hook.create_bucket(bucket_name=s3_bucket)
        else:
            logger.info("bucket %s exists", s3_bucket)

        return s3_bucket

    @task
    def github_archive_to_s3(s3_bucket, s3_key, **context):
        s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
        logical_date: pendulum.Datetime = context["logical_date"]
        date_partition = logical_date.strftime("%Y-%m-%d")
        hour_partition = logical_date.strftime("%H")
        s3_key = f"{s3_key}/{date_partition}/{hour_partition}/events.json.gz"

        if s3_hook.check_for_key(bucket_name=s3_bucket, key=s3_key):
            logger.info("s3 key %s already exists skipping", s3_key)
            return s3_key

        logger.info("writing to s3 %s %s", s3_bucket, s3_key)
        try:
            github_data = _get_github_archive(logical_date)
            # check size
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

        return s3_key

    @task
    def clean_raw_data(s3_bucket, s3_key, **context):
        logger.debug("cleaning raw data for %s %s", s3_bucket, s3_key)
        aws_access_key_id = "USERNAME"
        aws_secret_access_key = "PASSWORD"
        endpoint_url = "host.docker.internal:9000" #"localhost:9000"
        con = duckdb.connect()
        con.install_extension("httpfs")
        con.load_extension("httpfs")
        con.execute(f"SET s3_access_key_id='{aws_access_key_id}'")
        con.execute(f"SET s3_secret_access_key='{aws_secret_access_key}'")
        con.execute(f"SET s3_endpoint='{endpoint_url}'")
        con.execute("SET s3_use_ssl=false")
        con.execute("SET s3_url_style = 'path'")
        source_path = f"s3://{s3_bucket}/{s3_key}"
        con.execute(f"CREATE OR REPLACE TABLE raw AS FROM read_json_auto('{source_path}', ignore_errors=true)")

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
        con.execute(f"CREATE OR REPLACE TABLE clean AS FROM ({query})")
        table = con.table("clean")
        destination_path = source_path.replace("raw", "clean")
        destination_path = destination_path.replace(".json.gz", ".parquet")
        table.write_parquet(destination_path)
        con.close()


    create_s3_task = create_s3_bucket(S3_BUCKET)
    load_gh_archive_task = github_archive_to_s3(s3_bucket=S3_BUCKET, s3_key=S3_KEY)
    clean_raw_data_task = clean_raw_data(s3_bucket=S3_BUCKET, s3_key=load_gh_archive_task)
    create_s3_task >> load_gh_archive_task  >> clean_raw_data_task


github_archive_pipeline()

