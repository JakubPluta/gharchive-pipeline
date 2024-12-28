import sys
from logging import getLogger

import pendulum
from airflow.decorators import task, dag
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator


logger = getLogger(__name__)

SQL_CONN_ID = "postgres_default"

SQL_TEMPLATES_PATH = ["include/sql/"]
CLEAN_PG_TABLE_NAME = "clean_gharchive"


@dag(
    start_date=pendulum.today("UTC").subtract(days=2),
    schedule_interval="0 0 * * *",  # Midnight
    catchup=True,
    tags=["github"],
    max_active_runs=1,
    concurrency=1,
    template_searchpath=SQL_TEMPLATES_PATH,
    params={"table": CLEAN_PG_TABLE_NAME},
)
def github_daily_aggregators_dag():

    create_daily_event_summary_table = PostgresOperator(
        task_id="create_daily_event_summary_table",
        postgres_conn_id=SQL_CONN_ID,
        sql="create_daily_event_summary_table.sql",
    )

    create_repo_activity_summary_table = PostgresOperator(
        task_id="create_repo_activity_summary_table",
        postgres_conn_id=SQL_CONN_ID,
        sql="create_repo_activity_summary_table.sql",
    )

    create_clean_gharchive_table = PostgresOperator(
        task_id="create_clean_gharchive_table",
        postgres_conn_id=SQL_CONN_ID,
        sql="create_user_activity_summary_table.sql",
    )

    insert_daily_event_summary = PostgresOperator(
        task_id="insert_daily_event_summary",
        postgres_conn_id=SQL_CONN_ID,
        sql="insert_daily_event_summary.sql",
    )

    insert_repo_activity_summary = PostgresOperator(
        task_id="insert_repo_activity_summary",
        postgres_conn_id=SQL_CONN_ID,
        sql="insert_repo_activity_summary.sql",
    )

    insert_user_activity_summary = PostgresOperator(
        task_id="insert_user_activity_summary",
        postgres_conn_id=SQL_CONN_ID,
        sql="insert_user_activity_summary.sql",
    )

    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end")
    merge = DummyOperator(task_id="merge")
    _ = (
        start
        >> [
            create_daily_event_summary_table,
            create_repo_activity_summary_table,
            create_clean_gharchive_table,
        ]
        >> merge
        >> [
            insert_daily_event_summary,
            insert_repo_activity_summary,
            insert_user_activity_summary,
        ]
        >> end
    )


github_daily_aggregators_dag()
