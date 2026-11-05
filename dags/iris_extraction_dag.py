"""
Iris Extraction - dags/iris_extraction_dag.py

Runs extract.py every 3 hours inside the shared Airflow
(the one defined in iris_project_data_generator/docker-compose.yaml).

That Airflow mounts this project at /opt/airflow/extraction
and this dags/ folder at /opt/airflow/dags/extraction.

Why BashOperator and not PythonOperator:
    Both projects have a config.py. Running extract.py as its own process from its
    own folder guarantees `import config` loads THIS project's config, not the generator's.
    Any exception in extract.py -> non-zero exit code -> task FAILED in Airflow.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "kamal",
    "retries": 2,                          # a retry resumes the unfinished claim, no duplicates
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id="mlops_iris_extraction",
    default_args=default_args,
    start_date=datetime(2026, 10, 29),
    schedule="0 */3 * * *",                # every 3 hours (UTC)
    catchup=False,                         # the checkpoint already catches up; no backfill runs
    max_active_runs=1,                     # two extractors at once could claim overlapping ranges
    tags=["mlops_iris", "extraction"],
)

extract_task = BashOperator(
    task_id="extract_raw_to_csv",
    bash_command="cd /opt/airflow/extraction && python extract.py",
    dag=dag,
)
