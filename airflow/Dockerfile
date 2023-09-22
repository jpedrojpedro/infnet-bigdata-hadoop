FROM ubuntu:jammy

ARG AIRFLOW_HOME=/home/airflow

ENV AIRFLOW_HOME=${AIRFLOW_HOME}

RUN apt-get update
RUN apt-get install -y python3 pip

# Installing the other dependencies
RUN DEBIAN_FRONTEND=noninteractive apt-get install -y netcat-openbsd curl unzip awscli gcc python3-dev libpq-dev procps sudo htop nano libsasl2-dev

RUN useradd -ms /bin/bash -d ${AIRFLOW_HOME} airflow \
 && usermod -aG sudo airflow \
 && echo  "airflow     ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers \
 && pip install apache-airflow[celery,redis,postgres,crypto]==2.5.1 \
        apache-airflow-providers-amazon==6.1.0 \
        apache-airflow-providers-slack==5.1.0 \
        apache-airflow-providers-ssh==3.2.0 \
        apache-airflow-providers-apache-hive==4.0.0 \
        apache-airflow-providers-presto==4.0.1

WORKDIR ${AIRFLOW_HOME}

COPY entrypoint.sh entrypoint.sh
COPY config/variables.json variables.json
COPY config/setup_connections.py setup_connections.py
COPY config/requirements.txt requirements.txt
COPY config/webserver_config.py webserver_config.py
COPY plugins/ plugins
COPY dags/ dags

RUN echo "alias python=/usr/bin/python3\n" >> .bash_aliases
RUN chmod +x entrypoint.sh && chown -R airflow:airflow ${AIRFLOW_HOME}

USER airflow
EXPOSE 8080 5555
ENTRYPOINT ["./entrypoint.sh"]

