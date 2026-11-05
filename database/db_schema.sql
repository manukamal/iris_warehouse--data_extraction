-- =============================================================
-- MLOps Iris - Phase 2 (Extraction) - database/db_schema.sql
--
-- Run ONCE as root in MySQL Workbench (plain lightning bolt = run whole script).
-- Safe to re-run: every statement uses IF NOT EXISTS.
--
-- Replace CHANGE_ME_READER and CHANGE_ME_ETL with real passwords when you run it.
-- They must match SOURCE_PASSWORD and WAREHOUSE_PASSWORD in .env.
-- NEVER commit real passwords. They belong only in .env.
-- =============================================================


-- -------------------------------------------------------------
-- PART 1: SOURCE DATABASE (mlops_iris)
-- Owned by the generator project. This project only READS it.
-- We do NOT create raw_incoming_data here - the generator repo owns that table.
-- -------------------------------------------------------------

-- iris_reader: the extractor logs in with this to read source data.
-- SELECT only: it cannot insert, update, delete or drop anything in mlops_iris.
CREATE USER IF NOT EXISTS 'iris_reader'@'%' IDENTIFIED BY 'CHANGE_ME_READER';
GRANT SELECT ON mlops_iris.* TO 'iris_reader'@'%';


-- -------------------------------------------------------------
-- PART 2: WAREHOUSE DATABASE (iris_warehouse)
-- Owned by this project. Tracks every extraction run.
-- -------------------------------------------------------------

CREATE DATABASE IF NOT EXISTS iris_warehouse;
USE iris_warehouse;

-- One row per extraction file.
-- The checkpoint = MAX(end_id) WHERE status = 'SUCCESS'.
-- A file without a SUCCESS row does not count.
CREATE TABLE IF NOT EXISTS extraction_log (
    run_id         VARCHAR(64)   NOT NULL PRIMARY KEY,   -- e.g. 20261029_143000_a1b2c3
    file_name      VARCHAR(255)  NOT NULL UNIQUE,        -- e.g. raw_20261029_143000_a1b2c3.csv
    file_path      VARCHAR(500)  NOT NULL,               -- relative to data root, e.g. raw/2026-10-29/raw_20261029_143000_a1b2c3.csv
    start_id       BIGINT        NOT NULL,               -- first source id in this file
    end_id         BIGINT        NOT NULL,               -- last source id in this file
    batch_count    INT           NULL,                   -- batches written (max 500)
    row_count      INT           NULL,                   -- rows written (~24,000 max)
    status         ENUM('STARTED', 'SUCCESS', 'FAILED') NOT NULL DEFAULT 'STARTED',
    started_at     DATETIME      NOT NULL DEFAULT (UTC_TIMESTAMP()),   -- always UTC
    finished_at    DATETIME      NULL,                                 -- UTC, set on SUCCESS/FAILED
    error_message  TEXT          NULL,

    CONSTRAINT chk_id_range CHECK (end_id >= start_id),
    INDEX idx_status_end_id (status, end_id)            -- makes the checkpoint lookup fast
);

-- iris_etl: the extractor logs in with this to write the log.
-- No DELETE, no DROP: run history can never be erased by the app.
CREATE USER IF NOT EXISTS 'iris_etl'@'%' IDENTIFIED BY 'CHANGE_ME_ETL';
GRANT SELECT, INSERT, UPDATE ON iris_warehouse.* TO 'iris_etl'@'%';

FLUSH PRIVILEGES;


-- -------------------------------------------------------------
-- PART 3: VERIFY
-- -------------------------------------------------------------
DESCRIBE iris_warehouse.extraction_log;
SHOW GRANTS FOR 'iris_reader'@'%';
SHOW GRANTS FOR 'iris_etl'@'%';
