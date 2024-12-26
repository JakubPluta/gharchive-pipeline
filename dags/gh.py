import datetime
import io
from logging import getLogger
import requests
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow import DAG
from airflow.decorators import task, dag
from airflow.utils.dates import days_ago

logger = getLogger(__name__)


AWS_CONN_ID = "aws_default"
S3_BUCKET = "github-archive"
S3_KEY = "events"

BASE_URL = "http://data.gharchive.org"


def _prepare_date_string(date: datetime.datetime):
    fmt = "%Y-%m-%d-%-H"
    return date.strftime(fmt)


def _get_github_archive(date: datetime.datetime):
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
    start_date=days_ago(n=0, hour=3),
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
        logical_date = context["logical_date"]

        date_partition = datetime.datetime.strftime(logical_date, "%Y-%m-%d")
        hour_partition = datetime.datetime.strftime(logical_date, "%H")

        s3_key = f"{s3_key}/{date_partition}/{hour_partition}/events.json.gz"

        if s3_hook.check_for_key(bucket_name=s3_bucket, key=s3_key):
            logger.info("s3 key %s already exists skipping", s3_key)
            return

        logger.info("writing to s3 %s %s", s3_bucket, s3_key)
        try:
            github_data = _get_github_archive(logical_date)
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

    create_s3_task = create_s3_bucket(S3_BUCKET)
    load_gh_archive_task = github_archive_to_s3(s3_bucket=S3_BUCKET, s3_key=S3_KEY)

    create_s3_task >> load_gh_archive_task


github_archive_pipeline()

