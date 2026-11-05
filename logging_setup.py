"""
Iris Extraction - logging_setup.py
Logs to console (Airflow shows this) and to a rotating file.
"""

import logging
from logging.handlers import RotatingFileHandler

from config import LOG_DIR

LOG_DIR.mkdir(parents=True, exist_ok=True)

logger = logging.getLogger("iris_extraction")

if not logger.handlers:
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

    file_handler = RotatingFileHandler(LOG_DIR / "extraction.log", maxBytes=1_000_000, backupCount=3)
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
