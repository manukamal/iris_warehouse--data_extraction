"""
Iris Extraction - database/connection.py

Two engines, two logins, one job each:
    get_source_engine()     -> mlops_iris     as iris_reader (SELECT only)
    get_warehouse_engine()  -> iris_warehouse as iris_etl    (SELECT, INSERT, UPDATE)

Each engine is created once per process and reused (connection pooling).
"""

from sqlalchemy import create_engine
from sqlalchemy.engine import URL

from config import SOURCE_DB_CONFIG, WAREHOUSE_DB_CONFIG

_engines = {}


def _build_engine(cfg):
    # URL.create escapes the password, so characters like @ or : do not break the URL
    url = URL.create(
        drivername="mysql+pymysql",
        username=cfg["user"],
        password=cfg["password"],
        host=cfg["host"],
        port=cfg["port"],
        database=cfg["database"],
        query={"charset": "utf8mb4"},
    )
    return create_engine(
        url,
        pool_pre_ping=True,   # test the connection before using it (survives MySQL restarts)
        pool_recycle=3600,    # replace connections older than 1 hour
    )


def get_source_engine():
    """Read-only engine for mlops_iris."""
    if "source" not in _engines:
        _engines["source"] = _build_engine(SOURCE_DB_CONFIG)
    return _engines["source"]


def get_warehouse_engine():
    """Read/write engine for iris_warehouse."""
    if "warehouse" not in _engines:
        _engines["warehouse"] = _build_engine(WAREHOUSE_DB_CONFIG)
    return _engines["warehouse"]
