import logging
import pandas as pd
import datetime as dt
from pathlib import Path
from airflow.models import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator


log = logging.getLogger(__name__)
DEFAULT_ARGS = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=5),
}

with DAG(
    '04_mixed_ops',
    default_args=DEFAULT_ARGS,
    schedule_interval=None,
    start_date=dt.datetime(2021, 1, 1),
    catchup=False,
    tags=['pg-data', 'multiple-ops'],
) as dag:
    # sequence = []

    @task
    def extract_drivers():
        output_path = Path("dags/output")
        input_path = Path("dags/input")
        # log.info("opening postgres connection")
        # postgres_hook = PostgresHook(postgres_conn_id="pg-data")
        log.info("reading dataframe drivers")
        df = pd.read_csv(input_path / "f1" / "drivers.csv", encoding="iso-8859-1")
        log.info("persisting in XCOM the number of lines")
        return df.shape[0]

    bo = BashOperator(
        task_id="display_total_drivers",
        bash_command="echo Total drivers {{ task_instance.xcom_pull(task_ids='extract_drivers', key='return_value') }} ",
    )

    po1 = PostgresOperator(
        task_id="create_table",
        postgres_conn_id="pg-data",
        sql="""
        create table if not exists total_drivers(
            total int not null
        )
        """
    )

    po2 = PostgresOperator(
        task_id="insert_data",
        postgres_conn_id="pg-data",
        sql="""
            insert into total_drivers(total)
            values ({{ task_instance.xcom_pull(task_ids='extract_drivers', key='return_value') }} )
            """
    )

    po1 >> extract_drivers() >> [bo, po2]
