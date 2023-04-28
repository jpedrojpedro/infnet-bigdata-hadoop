import sys
import json
import configparser
from airflow import settings
from airflow.models import Connection


def parse_args():
    if len(sys.argv) > 1:
        return sys.argv[1]
    return None


def persist_conn(conn_id, conn_type=None, host=None, login=None, password=None, port=None, schema=None, extra=None):
    conn = Connection(
        conn_id=conn_id,
        conn_type=conn_type,
        host=host,
        login=login,
        password=password,
        port=port,
        schema=schema,
        extra=extra,
    )
    session = settings.Session
    session.add(conn)
    session.commit()


def main():
    arg = parse_args()
    if not arg:
        config = configparser.ConfigParser()
        config.read("connections.ini")
        for section in config.sections():
            persist_conn(
                conn_id=section,
                conn_type=config.get(section, "conn_type", fallback=None),
                host=config.get(section, "host", fallback=None),
                login=config.get(section, "login", fallback=None),
                password=config.get(section, "password", fallback=None),
                port=config.get(section, "port", fallback=None),
                schema=config.get(section, "schema", fallback=None),
                extra=config.get(section, "extra", fallback=None),
            )
        return
    conns = json.loads(arg)
    for conn_id, conn_params in conns.items():
        persist_conn(
            conn_id=conn_id,
            conn_type=conn_params.get("conn_type", None),
            host=conn_params.get("host", None),
            login=conn_params.get("login", None),
            password=conn_params.get("password", None),
            port=conn_params.get("port", None),
            schema=conn_params.get("schema", None),
            extra=conn_params.get("extra", None),
        )


if __name__ == '__main__':
    main()

