from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator


def get_clickhouse_client(dbname: str | None = None):
    """
    Tạo ClickHouse client.
    - dbname != None: set database mặc định = dbname
    - dbname == None: không set database (dùng để query toàn bộ databases)
    """
    print(">>> Starting ClickHouse client init...")

    try:
        import clickhouse_connect
    except ImportError as e:
        print("clickhouse_connect not installed:", e)
        raise

    host = "host.docker.internal"
    port = 8123
    user = "ch"
    password = "secret"
    secure = False

    kwargs = dict(
        host=host,
        port=port,
        username=user,
        password=password,
        secure=secure,
    )

    if dbname:
        kwargs["database"] = dbname
        print(f">>> Connecting to ClickHouse with default database='{dbname}'")
    else:
        print(">>> Connecting to ClickHouse without default database")

    print(f">>> host={host}, port={port}, user={user}, secure={secure}")
    return clickhouse_connect.get_client(**kwargs)


def list_clickhouse_databases():
    client = get_clickhouse_client()  # không set default DB

    result = client.query("SHOW DATABASES")
    dbs = [row[0] for row in result.result_rows]

    print(">>> ClickHouse databases:")
    for d in dbs:
        print(" -", d)

    return dbs


def list_tables_in_gold():
    client = get_clickhouse_client("gold")  # set default DB = gold

    result = client.query("SHOW TABLES")
    tables = [row[0] for row in result.result_rows]

    print(">>> Tables in database `gold`:")
    if not tables:
        print(" (no tables found)")
    else:
        for t in tables:
            print(" -", t)

    return tables


with DAG(
    dag_id="list_clickhouse_databases_and_gold_tables",
    start_date=datetime(2024, 1, 1),
    schedule=None,   # Airflow 3.x dùng schedule
    catchup=False,
    tags=["clickhouse", "debug"],
) as dag:

    task_list_dbs = PythonOperator(
        task_id="list_dbs",
        python_callable=list_clickhouse_databases,
    )

    task_list_tables_gold = PythonOperator(
        task_id="list_tables_gold",
        python_callable=list_tables_in_gold,
    )

    task_list_dbs >> task_list_tables_gold
