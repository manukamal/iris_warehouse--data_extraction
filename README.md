# iris_project_extraction

MLOps Iris, Phase 2. Copies new rows from `mlops_iris.raw_incoming_data` into CSV files
(up to 500 whole batches, ~24,000 rows, per file). Every file is tracked in
`iris_warehouse.extraction_log`, which is the only source of truth for the checkpoint.

## Setup (once)
1. Run `database/db_schema.sql` as root in MySQL Workbench. Replace `CHANGE_ME_READER`
   and `CHANGE_ME_ETL` with the passwords you put in `.env`.
2. `uv sync`
3. `uv run python -m database.test_connection`

## Run
    uv run python extract.py

Output: `data/raw/<YYYY-MM-DD>/raw_<run_id>.csv`. Logs: `logs/extraction.log`.

## Structure
    config.py               every setting: both DB configs, tables, paths, extraction limits
    logging_setup.py        console + rotating logs/extraction.log
    extract.py              the extractor (checkpoint -> claim -> .tmp -> rename -> SUCCESS)
    database/db_schema.sql  users + iris_warehouse + extraction_log
    database/connection.py  get_source_engine() (iris_reader) / get_warehouse_engine() (iris_etl)
    database/test_connection.py  proves both logins and the read-only permission
