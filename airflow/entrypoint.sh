#!/usr/bin/env bash

TRY_LOOP="20"

wait_for_port() {
  local name="$1" host="$2" port="$3"
  local j=0
  while ! nc -z "$host" "$port" >/dev/null 2>&1 < /dev/null; do
    j=$((j+1))
    if [ $j -ge $TRY_LOOP ]; then
      echo >&2 "$(date) - $host:$port still not reachable, giving up"
      exit 1
    fi
    echo "$(date) - waiting for $name $host... $j/$TRY_LOOP"
    sleep 5
  done
}

create_airflow_user() {
  airflow users create \
  --username "$AIRFLOW_USER_NAME" \
  --firstname "$AIRFLOW_USER_FIRST_NAME" \
  --lastname "$AIRFLOW_USER_LAST_NAME" \
  --role "$AIRFLOW_USER_ROLE" \
  --email "$AIRFLOW_USER_EMAIL" \
  --password "$AIRFLOW_USER_PASSWORD"
}

setup_airflow_variables() {
    if [ -e "variables.json" ]; then
      echo "Start importing Airflow variables"
      airflow variables import variables.json
    fi
}

setup_airflow_connections() {
    if [ -e "connections.ini" ]; then
      echo "Start setting up Airflow connections (file)"
      python3 setup_connections.py
    elif [[ -n "$AIRFLOW_CONNECTIONS" ]]; then
      echo "Start setting up Airflow connections (env_var)"
      python3 setup_connections.py "$AIRFLOW_CONNECTIONS"
    fi
}

install_python_packages() {
    if [ -e "requirements.txt" ]; then
      echo "Installing additional Python packages"
      pip3 install -r requirements.txt
    fi
}

wait_for_port "Postgres" "$POSTGRES_HOST" "$POSTGRES_PORT"
wait_for_port "Redis" "$REDIS_HOST" "$REDIS_PORT"

export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="postgresql://$POSTGRES_USER:$POSTGRES_PASSWORD@$POSTGRES_HOST:$POSTGRES_PORT/$POSTGRES_DB"
export AIRFLOW__CELERY__RESULT_BACKEND="db+postgresql://$POSTGRES_USER:$POSTGRES_PASSWORD@$POSTGRES_HOST:$POSTGRES_PORT/$POSTGRES_DB"
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__CORE__EXECUTOR=CeleryExecutor
export AIRFLOW__CORE__FERNET_KEY="jhs9Hz7VfxWwxZfsq7CqWyh2ZGVl_xx2dxROFK2NY9o="

case "$1" in
    webserver)
        airflow db init
        sleep 15
        create_airflow_user
        setup_airflow_variables
        setup_airflow_connections
        install_python_packages
        exec airflow webserver
        ;;
    worker)
        install_python_packages
        exec airflow celery "$@" -q "$QUEUE_NAMES"
        ;;
    scheduler)
        install_python_packages
        exec airflow scheduler
        ;;
    flower)
        exec airflow celery "$@"
        ;;
    *)
        exec "$@"
        ;;
esac
