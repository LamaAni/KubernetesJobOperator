from airflow import DAG
from src.kubernetes_job_operator import KubernetesBaseJobOperator
from airflow.operators.bash_operator import BashOperator
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


job_yaml = ""
with open(__file__ + ".yaml", "r", encoding="utf-8") as job_yaml_reader:
    job_yaml = job_yaml_reader.read()

bash_task = BashOperator(bash_command="date", task_id="test-bash", dag=dag)

job_task = KubernetesBaseJobOperator(task_id="test-job", job_yaml=job_yaml, dag=dag)

bash_task >> job_task

