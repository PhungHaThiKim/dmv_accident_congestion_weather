import os
import pandas as pd
import clickhouse_connect
from pathlib import Path


# ========= 1) Cấu hình ClickHouse =========
CLICKHOUSE_CONF = {
    "host": "localhost",   
    "port": 8123,          # HTTP port
    "username": "ch",
    "password": "secret",
    "secure": False,
}


# -------------- CONFIG --------------
HOST = "localhost"
PORT = 8123          # HTTP port (default 8123)
USER = "ch"
PASSWORD = "secret"
DATABASE = "bronze"

SQL_FILE = ["src/clickhouse/raw_accidents.sql",
             "src/clickhouse/raw_congestion.sql",
             "src/clickhouse/raw_weather.sql",
             "src/clickhouse/silver_ddl.sql",
             "src/clickhouse/gold_ddl.sql"]
# -----------------------------------

def split_sql_statements(sql_text: str):
    """
    Tách statement theo dấu ; nhưng bỏ qua comment và dòng trống.
    Cách này đủ tốt cho đa số file SQL không chứa ; trong string.
    """
    lines = []
    for line in sql_text.splitlines():
        s = line.strip()
        if not s or s.startswith("--"):
            continue
        lines.append(line)
    cleaned = "\n".join(lines)
    # tách theo ; và bỏ mảnh rỗng
    return [stmt.strip() for stmt in cleaned.split(";") if stmt.strip()]

def main():
    for path in SQL_FILE:
        sql_path = Path(path)
        if not sql_path.exists():
            raise FileNotFoundError(f"Không thấy file SQL: {sql_path.resolve()}")

        sql_text = sql_path.read_text(encoding="utf-8")
        statements = split_sql_statements(sql_text)

        client = clickhouse_connect.get_client(
            host=HOST,
            port=PORT,
            username=USER,
            password=PASSWORD,
            database=DATABASE,
        )

        for i, stmt in enumerate(statements, 1):
            print(f"\n--- Running statement {i}/{len(statements)} ---")
            print(stmt[:300] + ("..." if len(stmt) > 300 else ""))
            client.command(stmt)

        print(f"\n✅ Done:  {path}.")

if __name__ == "__main__":
    main()