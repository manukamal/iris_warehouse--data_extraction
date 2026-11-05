"""
Iris Extraction - database/test_connection.py

Checks both logins and proves the permissions are what we expect.
Run from the project root:
    uv run python -m database.test_connection
"""

from sqlalchemy import text

from database.connection import get_source_engine, get_warehouse_engine

# 1. iris_reader can read the source
with get_source_engine().connect() as conn:
    print("1. source rows:", conn.execute(text("SELECT COUNT(*) FROM raw_incoming_data")).scalar())

# 2. iris_etl can read the warehouse log
with get_warehouse_engine().connect() as conn:
    print("2. extraction_log rows:", conn.execute(text("SELECT COUNT(*) FROM extraction_log")).scalar())

# 3. iris_reader must NOT be able to write (expect error 1142: INSERT command denied)
try:
    with get_source_engine().begin() as conn:
        conn.execute(text("INSERT INTO raw_incoming_data (batch_id) VALUES ('should_fail')"))
    print("3. PROBLEM: iris_reader was able to write!")
except Exception as e:
    print("3. write blocked as expected:", e.orig.args if hasattr(e, "orig") else e)
