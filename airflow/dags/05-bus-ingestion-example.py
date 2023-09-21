import logging
import pandas as pd
import datetime as dt
from pathlib import Path
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
}

with DAG(
    '05_bus_ingestion_example',
    default_args=DEFAULT_ARGS,
    schedule_interval=None,
    start_date=dt.datetime(2023, 1, 1),
    catchup=False,
    tags=['example', 'pg-data'],
) as dag:
    sequence = []
    bus_api_path = Path(__file__).parent / "input" / "bus-api"
    patterns = ["2023-05-15_*.jsonl", "2023-05-16_*.jsonl", "2023-05-17_*.jsonl"]

    @task
    def ingestion(folder_path, pattern):
        log.info("starting ingestion task")
        log.info(f"folder location: {str(folder_path)}")
        log.info(f"checking folder: {folder_path.exists()}")
        glob_r = list(folder_path.glob(pattern))
        log.info(f"glob result: {glob_r}")
        log.info(f"glob amount: {len(glob_r)}")
        for filename in glob_r:
            log.info("opening postgres connection")
            postgres_hook = PostgresHook(postgres_conn_id="pg-data")
            log.info(f"reading dataframe {str(filename)}")
            for df_chunk in pd.read_json(filename, lines=True, chunksize=50_000):
                log.info(f"loading data into postgres table bus.report_full")
                df = df_chunk[
                    ["ordem", "latitude", "longitude", "velocidade", "linha", "datahoraservidor"]
                ].rename(columns={"latitude": "lat", "longitude": "long", "datahoraservidor": "dt"})
                df["lat"] = df["lat"].str.replace(",", ".").astype(float)
                df["long"] = df["long"].str.replace(",", ".").astype(float)
                df["velocidade"] = df["velocidade"].astype("int32")
                df["dt"] = pd.to_datetime(df["dt"], unit="ms")
                df.to_sql(
                    schema="bus",
                    name="report_full",
                    con=postgres_hook.get_sqlalchemy_engine(),
                    index=False,
                    if_exists="append"
                )

    for p in patterns:
        sequence.append(ingestion(bus_api_path, p))
    chain(*sequence)
