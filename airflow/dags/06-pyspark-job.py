import logging
import datetime as dt
from airflow import DAG
from airflow.utils.helpers import chain
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.sftp.operators.sftp import SFTPOperator, SFTPOperation
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


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
    dag_id='06_spark_etl_orchestration',
    default_args=DEFAULT_ARGS,
    # schedule_interval='@daily',
    schedule_interval=None,
    start_date=dt.datetime(2023, 1, 1),
    catchup=False,
    tags=['etl', 'spark', 'hdfs'],
) as dag:

    # copy_script_via_sftp = SFTPOperator(
    #     task_id='copy_script_via_sftp',
    #     ssh_conn_id='spark_ssh_connection',
    #     local_filepath='/home/airflow/dags/pyspark-jobs/spark_etl_script.py',
    #     remote_filepath='/tmp/spark_etl_script.py',
    #     operation=SFTPOperation.PUT,
    #     create_intermediate_dirs=True
    # )

    # copy_local_to_hdfs = SSHOperator(
    #     task_id='copy_local_to_hdfs',
    #     ssh_conn_id='spark_ssh_connection',
    #     command="""
    #         hdfs dfs -mkdir -p /user/root/mount/ && \
    #         hdfs dfs -put -f /tmp/spark_etl_script.py /user/root/pyspark-jobs/spark_etl_script.py
    #     """
    # )

    # run_spark_etl = SparkSubmitOperator(
    #     task_id='run_pyspark_etl',
    #     conn_id='spark_default',
    #     application='hdfs://namenode:9000/user/root/pyspark-jobs/spark_etl_script.py',
    #     name='airflow_spark_hdfs_etl',
    #     verbose=True,
    #     jars='/user/root/sqlite-jdbc-3.53.0.0.jar',
    #     application_args=[
    #         '--input-sqlite-hdfs-path', 'hdfs://namenode:9000/user/root/northwind.db',
    #         '--output-parquet-path', 'hdfs://namenode:9000/user/root/mount/orders/'
    #     ]
    # )

    run_spark_etl = SSHOperator(
        task_id='run_pyspark_etl',
        ssh_conn_id='spark_ssh_connection',
        command="""
            export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop && \
            export YARN_CONF_DIR=/opt/hadoop/etc/hadoop && \
            export PYSPARK_PYTHON=/usr/bin/python3 && \
            /opt/spark/bin/spark-submit \
            --master yarn \
            --deploy-mode client \
            --jars hdfs://node-master:9000/user/root/sqlite-jdbc-3.53.0.0.jar \
            --name airflow_spark_hdfs_etl \
            hdfs://node-master:9000/user/root/pyspark-jobs/spark_etl_script.py \
            --input-sqlite-path hdfs://node-master:9000/user/root/northwind.db \
            --output-parquet-path hdfs://node-master:9000/user/root/mount/orders/
        """
    )

    chain(run_spark_etl)
