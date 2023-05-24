import logging
import pandas as pd
import datetime as dt
from airflow.models import DAG
from airflow.decorators import task
from airflow.utils.helpers import chain
from airflow.providers.postgres.hooks.postgres import PostgresHook


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
    '03_data_ingestion',
    default_args=DEFAULT_ARGS,
    schedule_interval=None,
    start_date=dt.datetime(2021, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:
    sequence = []
    for tbl in ["circuits", "drivers", "qualifying"]:
        @task(task_id=f"extract_{tbl}")
        def extract(table_name):
            log.info("opening postgres connection")
            postgres_hook = PostgresHook(postgres_conn_id="pg-data")
            log.info(f"reading dataframe {table_name}")
            df = pd.read_csv(f"dags/input/f1/{table_name}.csv", encoding="iso-8859-1")
            log.info(f"loading data into postgres table {table_name}")
            df.to_sql(
                table_name,
                postgres_hook.get_sqlalchemy_engine(),
                if_exists="replace",
                chunksize=1000
            )
        sequence.append(extract(tbl))

    chain(*sequence)
