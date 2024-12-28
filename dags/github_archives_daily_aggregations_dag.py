import sys
from logging import getLogger

import pendulum
import requests
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.decorators import task, dag
from airflow.operators.dummy import DummyOperator
from airflow.providers.amazon.aws.transfers.s3_to_sql import S3ToSqlOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from gh.duckdb_repository import (
    DuckDBConfiguration,
    DuckDBRepository,
    get_default_duckdb_client_from_env,
)
from gh.utils import parse_parquet_generator, prepare_s3_partition_key


logger = getLogger(__name__)

SQL_CONN_ID = "postgres_default"

SQL_TEMPLATES_PATH = ["includes/sql/"]
CLEAN_PG_TABLE_NAME = "clean_gharchive"


@dag(
    start_date=pendulum.today("UTC").subtract(
        days=1
    ),  # for testing just load the last day
    schedule_interval="@daily",
    catchup=True,
    tags=["github"],
    max_active_runs=1,
    concurrency=1,
    template_searchpath=SQL_TEMPLATES_PATH,
)
def github_daily_aggregators_dag():

    create_daily_event_summary_table = PostgresOperator(
        task_id="create_daily_event_summary_table",
        postgres_conn_id=SQL_CONN_ID,
        sql="/sql/create_daily_event_summary_table.sql",
    )

    create_repo_activity_summary_table = PostgresOperator(
        task_id="create_repo_activity_summary_table",
        postgres_conn_id=SQL_CONN_ID,
        sql="/sql/create_repo_activity_summary_table.sql",
    )

    create_clean_gharchive_table = PostgresOperator(
        task_id="create_clean_gharchive_table",
        postgres_conn_id=SQL_CONN_ID,
        sql="/sql/create_clean_gharchive_table.sql",
    )

    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end")

    (
        start
        >> [
            create_daily_event_summary_table
            >> create_repo_activity_summary_table
            >> create_clean_gharchive_table
        ]
        >> end
    )
