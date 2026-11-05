"""
Iris Extraction - extract.py

Copies new rows from mlops_iris.raw_incoming_data into CSV files.
One file = up to BATCHES_PER_FILE whole batches. A batch is never split across files.
Every file gets one row in iris_warehouse.extraction_log.

Flow, per file:
    1. CHECKPOINT  highest end_id with status = SUCCESS (0 on the first run ever)
    2. RESUME      an unfinished (STARTED/FAILED) claim after the checkpoint? Redo that
                   exact range with the same run_id and file name. Overwrite, never duplicate.
    3. CLAIM       otherwise pick the next <= 500 batches and log them as STARTED
                   BEFORE writing anything
    4. EXTRACT     SELECT ... WHERE id BETWEEN start_id AND end_id, into a .tmp file
    5. PUBLISH     rename .tmp -> .csv only once it's fully written
    6. SUCCESS     mark the log row SUCCESS. Only now does the checkpoint move.
Repeats until caught up or MAX_FILES_PER_RUN is reached.

Rule: the log is the only source of truth. A .csv without a SUCCESS row doesn't count.

Assumes a single writer: the generator inserts one batch at a time in one transaction
(Airflow max_active_runs=1). Two generators at once would interleave ids.

Run from the project root:
    uv run python extract.py
"""

import csv
import uuid
from datetime import datetime, timezone

from sqlalchemy import text

from config import (
    BATCHES_PER_FILE,
    DATA_ROOT,
    EXTRACT_COLUMNS,
    MAX_FILES_PER_RUN,
    TABLE_EXTRACTION_LOG,
    TABLE_SOURCE_RAW,
)
from database.connection import get_source_engine, get_warehouse_engine
from logging_setup import logger


def utc_now():
    # MySQL DATETIME has no timezone, so store naive UTC (matches the table's UTC default)
    return datetime.now(timezone.utc).replace(tzinfo=None)


def new_run_names():
    """run_id, file_name and relative file_path for a new file."""
    now = utc_now()
    run_id = f"{now:%Y%m%d_%H%M%S}_{uuid.uuid4().hex[:6]}"
    file_name = f"raw_{run_id}.csv"
    file_path = f"raw/{now:%Y-%m-%d}/{file_name}"  # relative to DATA_ROOT
    return run_id, file_name, file_path


# ─────────────────────────────────────────────
# Step 1: checkpoint (warehouse)
# ─────────────────────────────────────────────
def get_checkpoint(wh_conn):
    sql = text(f"SELECT COALESCE(MAX(end_id), 0) FROM {TABLE_EXTRACTION_LOG} WHERE status = 'SUCCESS'")
    return wh_conn.execute(sql).scalar()


# ─────────────────────────────────────────────
# Step 2: unfinished claim (warehouse)
# ─────────────────────────────────────────────
def find_unfinished(wh_conn, checkpoint):
    """The earliest STARTED/FAILED claim after the checkpoint, if any."""
    sql = text(f"""
        SELECT run_id, file_name, file_path, start_id, end_id, batch_count
        FROM {TABLE_EXTRACTION_LOG}
        WHERE status IN ('STARTED', 'FAILED') AND start_id > :checkpoint
        ORDER BY start_id
        LIMIT 1
    """)
    return wh_conn.execute(sql, {"checkpoint": checkpoint}).mappings().first()


# ─────────────────────────────────────────────
# Step 3: next range of whole batches (source)
# ─────────────────────────────────────────────
def find_next_range(src_conn, checkpoint):
    """Return (start_id, end_id, batch_count) for the next <= BATCHES_PER_FILE batches, or None."""
    sql = text(f"""
        SELECT batch_id, MIN(id) AS first_id, MAX(id) AS last_id
        FROM {TABLE_SOURCE_RAW}
        WHERE id > :checkpoint
        GROUP BY batch_id
        ORDER BY first_id
        LIMIT :n
    """)
    batches = src_conn.execute(sql, {"checkpoint": checkpoint, "n": BATCHES_PER_FILE}).all()
    if not batches:
        return None
    start_id = batches[0].first_id
    end_id = max(b.last_id for b in batches)
    return start_id, end_id, len(batches)


def claim(wh_engine, run_id, file_name, file_path, start_id, end_id, batch_count):
    sql = text(f"""
        INSERT INTO {TABLE_EXTRACTION_LOG}
            (run_id, file_name, file_path, start_id, end_id, batch_count, status, started_at)
        VALUES
            (:run_id, :file_name, :file_path, :start_id, :end_id, :batch_count, 'STARTED', :now)
    """)
    with wh_engine.begin() as conn:
        conn.execute(sql, {
            "run_id": run_id, "file_name": file_name, "file_path": file_path,
            "start_id": start_id, "end_id": end_id, "batch_count": batch_count, "now": utc_now(),
        })


def reopen(wh_engine, run_id):
    """Put an unfinished claim back to STARTED before retrying it."""
    sql = text(f"""
        UPDATE {TABLE_EXTRACTION_LOG}
        SET status = 'STARTED', error_message = NULL, finished_at = NULL
        WHERE run_id = :run_id
    """)
    with wh_engine.begin() as conn:
        conn.execute(sql, {"run_id": run_id})


# ─────────────────────────────────────────────
# Steps 4 + 5: extract to .tmp, then rename (source)
# ─────────────────────────────────────────────
def extract_to_csv(src_engine, start_id, end_id, file_path):
    """Write rows start_id..end_id to DATA_ROOT/file_path. Returns the row count."""
    final_path = DATA_ROOT / file_path
    tmp_path = final_path.with_suffix(".csv.tmp")
    final_path.parent.mkdir(parents=True, exist_ok=True)

    sql = text(f"""
        SELECT {", ".join(EXTRACT_COLUMNS)}
        FROM {TABLE_SOURCE_RAW}
        WHERE id BETWEEN :start_id AND :end_id
        ORDER BY id
    """)

    row_count = 0
    with src_engine.connect() as conn, open(tmp_path, "w", newline="", encoding="utf-8") as f:
        # stream_results: rows come from MySQL in chunks, not all 24,000 into memory at once
        result = conn.execution_options(stream_results=True).execute(
            sql, {"start_id": start_id, "end_id": end_id}
        )
        writer = csv.writer(f)
        writer.writerow(EXTRACT_COLUMNS)
        for row in result:
            writer.writerow(row)
            row_count += 1

    if row_count == 0:
        tmp_path.unlink(missing_ok=True)
        raise RuntimeError(f"No rows found for ids {start_id}-{end_id}")

    tmp_path.replace(final_path)  # atomic rename; overwrites an old file on a retry
    return row_count


# ─────────────────────────────────────────────
# Step 6: close the log row (warehouse)
# ─────────────────────────────────────────────
def mark_success(wh_engine, run_id, row_count):
    sql = text(f"""
        UPDATE {TABLE_EXTRACTION_LOG}
        SET status = 'SUCCESS', row_count = :row_count, finished_at = :now
        WHERE run_id = :run_id AND status = 'STARTED'
    """)
    with wh_engine.begin() as conn:
        updated = conn.execute(sql, {"run_id": run_id, "row_count": row_count, "now": utc_now()}).rowcount
    if updated != 1:
        raise RuntimeError(f"Could not mark {run_id} SUCCESS (rows updated: {updated})")


def mark_failed(wh_engine, run_id, error):
    sql = text(f"""
        UPDATE {TABLE_EXTRACTION_LOG}
        SET status = 'FAILED', error_message = :error, finished_at = :now
        WHERE run_id = :run_id
    """)
    with wh_engine.begin() as conn:
        conn.execute(sql, {"run_id": run_id, "error": str(error)[:2000], "now": utc_now()})


# ─────────────────────────────────────────────
# One file
# ─────────────────────────────────────────────
def extract_one_file(src_engine, wh_engine):
    """Extract one file. Returns its row count, or 0 when there is nothing new."""
    with wh_engine.connect() as wh_conn:
        checkpoint = get_checkpoint(wh_conn)
        job = find_unfinished(wh_conn, checkpoint)

    if job:
        run_id, file_name, file_path = job["run_id"], job["file_name"], job["file_path"]
        start_id, end_id, batch_count = job["start_id"], job["end_id"], job["batch_count"]
        logger.warning("Resuming unfinished run | run_id=%s | ids=%s-%s", run_id, start_id, end_id)
        reopen(wh_engine, run_id)
    else:
        with src_engine.connect() as src_conn:
            next_range = find_next_range(src_conn, checkpoint)
        if next_range is None:
            logger.info("Nothing new after checkpoint id=%s", checkpoint)
            return 0
        start_id, end_id, batch_count = next_range
        run_id, file_name, file_path = new_run_names()
        claim(wh_engine, run_id, file_name, file_path, start_id, end_id, batch_count)
        logger.info("Claimed | run_id=%s | ids=%s-%s | batches=%s", run_id, start_id, end_id, batch_count)

    try:
        row_count = extract_to_csv(src_engine, start_id, end_id, file_path)
        mark_success(wh_engine, run_id, row_count)
    except Exception as e:
        logger.exception("Extraction failed | run_id=%s", run_id)
        try:
            mark_failed(wh_engine, run_id, e)
        except Exception:
            # Warehouse is down too. The row stays STARTED and the next run resumes it.
            logger.exception("Could not mark run FAILED | run_id=%s", run_id)
        raise

    logger.info("SUCCESS | run_id=%s | rows=%s | batches=%s | file=%s", run_id, row_count, batch_count, file_path)
    return row_count


# ─────────────────────────────────────────────
# One run = loop until caught up or capped
# ─────────────────────────────────────────────
def run():
    src_engine = get_source_engine()
    wh_engine = get_warehouse_engine()

    files, total_rows = 0, 0
    while files < MAX_FILES_PER_RUN:
        rows = extract_one_file(src_engine, wh_engine)
        if rows == 0:
            break
        files += 1
        total_rows += rows

    if files == MAX_FILES_PER_RUN:
        logger.warning("Hit MAX_FILES_PER_RUN=%s. Backlog remains; the next run continues.", MAX_FILES_PER_RUN)
    logger.info("Run finished | files=%s | rows=%s", files, total_rows)
    return files, total_rows


if __name__ == "__main__":
    run()  # any exception -> non-zero exit code -> Airflow marks the task failed
