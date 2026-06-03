FROM apache/airflow:2.5.1-python3.10

USER root
RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y netcat-openbsd curl unzip gcc g++ python3-dev libpq-dev procps sudo htop nano libsasl2-dev && \
    apt-get clean

RUN echo "airflow ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers

USER airflow

WORKDIR /home/airflow

COPY requirements.txt ./requirements.txt

RUN pip install --no-cache-dir -r ./requirements.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.5.1/constraints-3.10.txt"

COPY --chown=airflow:airflow entrypoint.sh entrypoint.sh
COPY --chown=airflow:airflow config/variables.json variables.json
COPY --chown=airflow:airflow config/setup_connections.py setup_connections.py
COPY --chown=airflow:airflow config/requirements.txt requirements.txt
COPY --chown=airflow:airflow config/webserver_config.py webserver_config.py
COPY --chown=airflow:airflow plugins/ plugins
COPY --chown=airflow:airflow dags/ dags

RUN chmod +x entrypoint.sh

EXPOSE 8080 5555
ENTRYPOINT ["./entrypoint.sh"]
