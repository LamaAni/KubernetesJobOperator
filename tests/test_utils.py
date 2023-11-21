from airflow import DAG
from datetime import datetime, timezone


def test_dag(dag: DAG):
    dag.clear()
    dag.test(datetime.now(tz=timezone.utc))
