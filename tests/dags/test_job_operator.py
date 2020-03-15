from airflow import DAG
from airflow_kubernetes_job_operator.kubernetes_job_operator import KubernetesJobOperator
from airflow.utils.dates import days_ago

default_args = {"owner": "tester", "start_date": days_ago(2), "retries": 0}
dag = DAG(
    "bjo", default_args=default_args, description="Test base job operator", schedule_interval=None
)


def read_job_yaml(fpath):
    job_yaml = ""
    with open(fpath, "r", encoding="utf-8") as job_yaml_reader:
        job_yaml = job_yaml_reader.read()
    return job_yaml


success_job_yaml = read_job_yaml(__file__ + ".success.yaml")
fail_job_yaml = read_job_yaml(__file__ + ".fail.yaml")

envs = {"PASS_ARG": "a test"}

# BashOperator(bash_command="date", task_id="test-bash", dag=dag)

KubernetesJobOperator(task_id="test-job-success", job_yaml=success_job_yaml, envs=envs, dag=dag)
KubernetesJobOperator(task_id="test-job-fail", job_yaml=fail_job_yaml, envs=envs, dag=dag)
KubernetesJobOperator(
    task_id="test-job-overrides",
    dag=dag,
    image="ubuntu",
    envs=envs,
    command=["bash", "-c", 'echo "Starting $PASS_ARG"; sleep 10; echo end'],
)

