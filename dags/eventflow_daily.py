"""Daily GH Archive pipeline.

Each task is one CLI command, so anything Airflow runs can be run by hand with the same result.
depends_on_past keeps days in order, which the Type 2 repo dimension relies on, and a failed
audit stops that day: load never runs, so the warehouse keeps serving the last good data.
"""
from __future__ import annotations

from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.python import PythonSensor

LAST_HOUR = "https://data.gharchive.org/{ds}-23.json.gz"


def _published(ds: str) -> bool:
    """The archive publishes each hour with a delay, so wait for the day's last file."""
    return requests.head(LAST_HOUR.format(ds=ds), timeout=30).status_code == 200


def cli(command: str, day: bool = True) -> str:
    return f"python -m eventflow.cli {command}" + (" --start {{ ds }}" if day else "")


with DAG(
    dag_id="eventflow_daily",
    schedule="0 3 * * *",
    start_date=datetime(2026, 8, 20),
    catchup=True,
    max_active_runs=1,
    default_args={"depends_on_past": True, "retries": 0},
    tags=["gharchive"],
) as dag:
    wait = PythonSensor(task_id="wait_for_source", python_callable=_published, op_kwargs={"ds": "{{ ds }}"},
                        mode="reschedule", poke_interval=600, timeout=6 * 3600)
    status = BashOperator(task_id="snapshot_incidents", bash_command=cli("status", day=False),
                          retries=3, retry_delay=timedelta(minutes=5))
    ingest = BashOperator(task_id="ingest", bash_command=cli("ingest"), retries=3, retry_delay=timedelta(minutes=10))
    # No retries: a failed audit is a data problem, and running it again would not change the answer.
    transform = BashOperator(task_id="transform_audit_publish", bash_command=cli("transform"))
    load = BashOperator(task_id="load", bash_command=cli("load"), retries=2, retry_delay=timedelta(minutes=2))
    marts = BashOperator(task_id="refresh_marts", bash_command=cli("marts", day=False))
    analyze = BashOperator(task_id="score_anomalies", bash_command=cli("analyze", day=False))

    wait >> [status, ingest]
    ingest >> transform >> load
    [status, load] >> marts >> analyze
