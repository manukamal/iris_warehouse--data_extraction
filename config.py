"""
Iris Extraction - config.py

Single source of truth for the extraction project.
Works locally (values from .env) and in Docker/Airflow (container env vars).
"""

import os
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # Docker: variables come from the container environment

PROJECT_ROOT = Path(__file__).parent


def _required(name):
    """Fail fast with a clear message if a required variable is missing."""
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"{name} is not set. Add it to .env (local) or the container environment (Docker).")
    return value


# ─────────────────────────────────────────────
# 1. MYSQL SERVER (shared by both databases)
# ─────────────────────────────────────────────
MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))

# ─────────────────────────────────────────────
# 2. SOURCE: mlops_iris, read-only via iris_reader
# ─────────────────────────────────────────────
SOURCE_DB_CONFIG = {
    "host": MYSQL_HOST,
    "port": MYSQL_PORT,
    "user": os.getenv("SOURCE_USER", "iris_reader"),
    "password": _required("SOURCE_PASSWORD"),
    "database": os.getenv("SOURCE_DATABASE", "mlops_iris"),
}

# ─────────────────────────────────────────────
# 3. WAREHOUSE: iris_warehouse, written via iris_etl
# ─────────────────────────────────────────────
WAREHOUSE_DB_CONFIG = {
    "host": MYSQL_HOST,
    "port": MYSQL_PORT,
    "user": os.getenv("WAREHOUSE_USER", "iris_etl"),
    "password": _required("WAREHOUSE_PASSWORD"),
    "database": os.getenv("WAREHOUSE_DATABASE", "iris_warehouse"),
}

# ─────────────────────────────────────────────
# 4. TABLES
# ─────────────────────────────────────────────
TABLE_SOURCE_RAW = "raw_incoming_data"     # in mlops_iris (owned by the generator repo)
TABLE_EXTRACTION_LOG = "extraction_log"    # in iris_warehouse (owned by this repo)

# ─────────────────────────────────────────────
# 5. PATHS
# ─────────────────────────────────────────────
DATA_ROOT = PROJECT_ROOT / "data"          # extraction_log.file_path is relative to this
RAW_DIR = DATA_ROOT / "raw"
LOG_DIR = PROJECT_ROOT / "logs"

# ─────────────────────────────────────────────
# 6. EXTRACTION
# ─────────────────────────────────────────────
# One CSV file = at most this many batches (500 x 48 = ~24,000 rows).
BATCHES_PER_FILE = 500

# Safety cap on files per run. The generator makes ~1,080 batches every 3 hours
# (~3 files), so 10 leaves room to catch up after downtime without one run going forever.
MAX_FILES_PER_RUN = 10

# Columns written to the CSV. Raw = faithful copy of the source.
# `processed` is left out: iris_reader can't update it, so it's always 0 and means nothing here.
# `generation_mode` stays in raw; drop it later at the feature step (a real model never sees it).
EXTRACT_COLUMNS = [
    "id",
    "batch_id",
    "sepal_length",
    "sepal_width",
    "petal_length",
    "petal_width",
    "species",
    "generation_mode",
    "created_at",
]
