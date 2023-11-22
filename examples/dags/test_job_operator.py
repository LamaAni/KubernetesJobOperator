from airflow import DAG
from airflow_kubernetes_job_operator.kubernetes_job_operator import (
    KubernetesJobOperator,
)
from airflow.utils.dates import days_ago

default_args = {"owner": "tester", "start_date": days_ago(2), "retries": 0}
dag = DAG(
    "job-operator-example",
    default_args=default_args,
    description="Test base job operator",
    schedule_interval=None,
)


def read_body(fpath):
    body = ""
    with open(fpath, "r", encoding="utf-8") as body_reader:
        body = body_reader.read()
    return body


success_body = read_body(__file__ + ".success.yaml")
fail_body = read_body(__file__ + ".fail.yaml")

# BashOperator(bash_command="date", task_id="test-bash", dag=dag)

KubernetesJobOperator(task_id="test-job-success", body=success_body, dag=dag)
KubernetesJobOperator(task_id="test-job-fail", body=fail_body, dag=dag)
KubernetesJobOperator(
    task_id="test-job-overrides",
    dag=dag,
    image="ubuntu",
    command=["bash", "-c", "echo start; sleep 10; echo end"],
)
