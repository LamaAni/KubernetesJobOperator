from airflow import DAG
from src.kubernetes_base_job_operator import KubernetesBaseJobOperator

# from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {"owner": "tester", "start_date": days_ago(2), "retries": 0}

dag = DAG(
    "bjo",
    default_args=default_args,
    description="Test base job operator",
    schedule_interval=None,
)


def read_job_yaml(fpath):
    job_yaml = ""
    with open(fpath, "r", encoding="utf-8") as job_yaml_reader:
        job_yaml = job_yaml_reader.read()
    return job_yaml


success_job_yaml = read_job_yaml(__file__ + ".success.yaml")
fail_job_yaml = read_job_yaml(__file__ + ".fail.yaml")

# BashOperator(bash_command="date", task_id="test-bash", dag=dag)

KubernetesBaseJobOperator(
    task_id="test-job-success", job_yaml=success_job_yaml, dag=dag
)

KubernetesBaseJobOperator(task_id="test-job-fail", job_yaml=fail_job_yaml, dag=dag)

