import os
from datetime import timedelta
from utils import default_args, name_from_file
from airflow import DAG
from airflow_kubernetes_job_operator.kubernetes_job_operator import (
    KubernetesJobOperator,
)

print(
    "\n".join([f"{k} = {v}" for k, v in os.environ.items() if k.startswith("AIRFLOW")])
)

dag = DAG(
    name_from_file(__file__),
    default_args=default_args,
    description="Test base job operator",
    schedule_interval=None,
    catchup=False,
)

envs = {
    "PASS_ARG": "a test",
}

total_time_seconds = round(timedelta(hours=4).total_seconds())

KubernetesJobOperator(
    task_id="test-long-job-success",
    body_filepath="./templates/test_long_job.yaml",
    envs={
        "PASS_ARG": "a long test",
        "TIC_COUNT": str(total_time_seconds),
    },
    dag=dag,
)


if __name__ == "__main__":
    from tests.test_utils import test_dag

    test_dag(dag)
