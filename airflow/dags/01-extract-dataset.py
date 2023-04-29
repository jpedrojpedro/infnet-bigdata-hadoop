import logging
import datetime as dt
from airflow.models import DAG
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.dummy import DummyOperator
# from airflow.operators.trigger_dagrun import TriggerDagRunOperator
# from airflow.providers.sqlite.operators.sqlite import SqliteOperator
# from airflow.providers.sqlite.hooks.sqlite import SqliteHook
# from utils.helper import apply_task_downstream


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

sequence = []

dag = DAG(
    '01_extract_dataset',
    default_args=DEFAULT_ARGS,
    description='A simple tutorial DAG',
    schedule_interval=None,
    start_date=dt.datetime(2021, 1, 1),
    catchup=False,
    tags=['example'],
)

#extract_artists_sqlite_task = SqliteOperator(
#    task_id='extract_artists',
#    sql='select * from artist',  # can refer to a file
#    sqlite_conn_id='chinook_db',
#    dag=dag,
#)
#sequence.append(extract_artists_sqlite_task)

conn = DummyOperator(
    task_id='connector',
    trigger_rule=TriggerRule.ALL_DONE,  # all directly upstream tasks are finished - failed or succeeded
    dag=dag,
)
sequence.append(conn)

#for table_name in ['Artist', 'Album']:
#    @dag.task(task_id=f"extract_table_{table_name}")
#    def extract_table():
#        sqlite_hook = SqliteHook(sqlite_conn_id='chinook_db')
#        df = sqlite_hook.get_pandas_df(sql=f'select * from {table_name}')
#        df.to_csv(f'/opt/airflow/outputs/{table_name}.csv', index=False)
#    sequence.append(extract_table())

#trigger_transformation = TriggerDagRunOperator(
#    task_id='trigger_transformation_dag',
#    trigger_dag_id='02_transform_data',
#    dag=dag,
#)
#sequence.append(trigger_transformation)

#apply_task_downstream(sequence)
