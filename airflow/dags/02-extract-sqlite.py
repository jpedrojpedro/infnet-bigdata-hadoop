import logging
import pandas as pd
import datetime as dt
from airflow.models import DAG
from airflow.decorators import task
# from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.providers.sqlite.hooks.sqlite import SqliteHook


log = logging.getLogger(__name__)
DEFAULT_ARGS = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}

with DAG(
    '02_extract_sqlite',
    default_args=DEFAULT_ARGS,
    schedule_interval=None,
    start_date=dt.datetime(2021, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:

    # extract_artists = SqliteOperator(
    #     task_id='extract_artists',
    #     sql='select * from artist',  # can refer to a file
    #     sqlite_conn_id='chinook_db',
    # )

    @task
    def extract_artist():
        sqlite_hook = SqliteHook(sqlite_conn_id='chinook_db')
        df = sqlite_hook.get_pandas_df(sql=f'select * from artist')
        df.to_csv(f'/home/airflow/dags/output/artist.csv', index=False)
        return df.shape[0]

    @task
    def extract_album():
        sqlite_hook = SqliteHook(sqlite_conn_id='chinook_db')
        df = sqlite_hook.get_pandas_df(sql=f'select * from album')
        df.to_csv(f'/home/airflow/dags/output/album.csv', index=False)
        return df.shape[0]

    @task
    def check_amount(total_artists, total_albums):
        log.info(f"====> Total artists: {total_artists}")
        log.info(f"====> Total albums: {total_albums}")

    check_amount(extract_artist(), extract_album())
