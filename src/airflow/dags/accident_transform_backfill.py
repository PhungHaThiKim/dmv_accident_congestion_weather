import os
from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from clickhouse_driver import Client

# -------------------------
# Base paths
# -------------------------
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
SQL_DIR = os.path.join(BASE_DIR, "airflow", "sql")


def run_sql_file(filename, ds_start=None, ds_end=None, **context):
    """
    Đọc file SQL trong thư mục SQL_DIR, format với {ds_start}, {ds_end}, ...
    rồi execute lần lượt từng statement (ngăn bởi ';') trên ClickHouse.
    """
    path = os.path.join(SQL_DIR, filename)
    with open(path, "r", encoding="utf-8") as f:
        sql_template = f.read()

    # Default cho placeholder thường dùng
    params = {
        "ds_start": ds_start or "",
        "ds_end": ds_end or "",
    }
    # Cho phép truyền thêm tham số khác qua op_kwargs
    params.update({k: v for k, v in context.items() if k not in ("templates_dict",)})

    sql = sql_template.format(**params)

    client = Client(
        host="host.docker.internal",
        port=9000,
        user="ch",
        password="secret",
        secure=False,
        settings={"use_numpy": False},
    )

    # Cho phép nhiều statement trong 1 file, phân tách bằng ';'
    statements = [s.strip() for s in sql.split(";") if s.strip()]
    for stmt in statements:
        client.execute(stmt)


default_args = {"owner": "you"}

with DAG(
    dag_id="accident_congestion_transform_backfill",
    start_date=datetime(2025, 11, 1),
    schedule="@once",
    catchup=False,
    default_args=default_args,
    tags=["clickhouse", "accidents", "gold", "silver"],
) as dag:

    # ---------------------------------
    # SILVER LAYER TRANSFORM
    # ---------------------------------
    with TaskGroup(group_id="silver_transform") as silver_transform:

        load_accidents = PythonOperator(
            task_id="load_silver_accidents",
            python_callable=run_sql_file,
            op_kwargs={
                "filename": "silver_accidents.sql",
                "ds_start": "2016-01-01",
                "ds_end": "2022-01-01",
            },
        )

        load_congestion = PythonOperator(
            task_id="load_silver_congestion",
            python_callable=run_sql_file,
            op_kwargs={
                "filename": "silver_congestion.sql",
                "ds_start": "2016-01-01",
                "ds_end": "2022-01-01",
            },
        )

        load_weather = PythonOperator(
            task_id="load_silver_weather",
            python_callable=run_sql_file,
            op_kwargs={
                "filename": "silver_weather.sql",
                "ds_start": "2016-01-01",
                "ds_end": "2022-01-01",
            },
        )

        # Không cần dependency giữa 3 cái này, cứ chạy song song
        # (nếu sau này cần thứ tự thì thêm: load_accidents >> load_congestion >> load_weather)


    # ---------------------------------
    # GOLD LAYER TRANSFORM
    # ---------------------------------
    with TaskGroup(group_id="gold_transform") as gold_transform:
        # ---------- DIMENSIONS ----------
        dim_datetime = PythonOperator(
            task_id="dim_datetime",
            python_callable=run_sql_file,
            op_kwargs={
                "filename": "gold_dim_datetime.sql",
            },
        )

        dim_location = PythonOperator(
            task_id="dim_location",
            python_callable=run_sql_file,
            op_kwargs={
                "filename": "gold_dim_location.sql",
            },
        )

        dim_airport = PythonOperator(
            task_id="dim_airport",
            python_callable=run_sql_file,
            op_kwargs={
                "filename": "gold_dim_airport.sql",
            },
        )

        dim_weather_type = PythonOperator(
            task_id="dim_weather_type",
            python_callable=run_sql_file,
            op_kwargs={
                "filename": "gold_dim_weather_type.sql",
            },
        )

        dim_city_climate = PythonOperator(
            task_id="dim_city_climate",
            python_callable=run_sql_file,
            op_kwargs={
                "filename": "gold_dim_city_climate.sql",
            },
        )

        dim_infrastructure = PythonOperator(
            task_id="dim_infrastructure",
            python_callable=run_sql_file,
            op_kwargs={
                "filename": "gold_dim_infrastructure.sql",
            },
        )

        # ---------- FACTS ----------
        fact_accident = PythonOperator(
            task_id="fact_accident",
            python_callable=run_sql_file,
            op_kwargs={
                "filename": "gold_fact_accident.sql",
            },
        )

        fact_congestion = PythonOperator(
            task_id="fact_congestion",
            python_callable=run_sql_file,
            op_kwargs={
                "filename": "gold_fact_congestion.sql",
            },
        )

        fact_weather_events = PythonOperator(
            task_id="fact_weather_events",
            python_callable=run_sql_file,
            op_kwargs={
                "filename": "gold_fact_weather_events.sql",
            },
        )

        # ---------- BRIDGES ----------
        bridge_accident_weather = PythonOperator(
            task_id="bridge_accident_weather",
            python_callable=run_sql_file,
            op_kwargs={
                "filename": "gold_bridge_accident_weather.sql",
            },
        )

        bridge_accident_congestion = PythonOperator(
            task_id="bridge_accident_congestion",
            python_callable=run_sql_file,
            op_kwargs={
                "filename": "gold_bridge_accident_congestion.sql",
            },
        )

        # ---------- Dependencies trong GOLD ----------
        dim_tasks = [
            dim_datetime,
            dim_location,
            dim_airport,
            dim_weather_type,
            dim_city_climate,
            dim_infrastructure,
        ]

        fact_tasks = [
            fact_accident,
            fact_congestion,
            fact_weather_events,
        ]

        bridge_tasks = [
            bridge_accident_weather,
            bridge_accident_congestion,
        ]

        # Tất cả DIM phải xong trước FACT
        for f in fact_tasks:
            f.set_upstream(dim_tasks)

        # Tất cả FACT phải xong trước BRIDGE
        for b in bridge_tasks:
            b.set_upstream(fact_tasks)

    # ---------------------------------
    # TOÀN DAG DEPENDENCY
    # ---------------------------------
    silver_transform >> gold_transform
