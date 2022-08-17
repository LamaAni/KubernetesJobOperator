from utils import default_args, name_from_file
from airflow import DAG
from datetime import datetime, date

from airflow.models import TaskInstance
from airflow.operators.python_operator import PythonOperator
from airflow_kubernetes_job_operator.kubernetes_legacy_job_operator import KubernetesJobOperator

dag = DAG(
    name_from_file(__file__),
    default_args=default_args,
    start_date=datetime.today(),
    description="Test base job operator",
    schedule_interval=None,
    catchup=False,
)


def print_xcom_pull(ti: TaskInstance, **context):
    xcom_value = ti.xcom_pull(task_ids="test_kube_api_event_with_xcom", key="a")
    ti.log.info(xcom_value)


with dag:
    kube_task = KubernetesJobOperator(
        task_id="test_kube_api_event_with_xcom",
        image="ubuntu:latest",
        command=[
            "echo",
            '::kube_api:xcom={"a":2,"b":"someval"}',
        ],
    )

    kube_task >> PythonOperator(
        task_id="test_pull_xcom",
        dag=dag,
        python_callable=print_xcom_pull,
        provide_context=True,
    )

if __name__ == "__main__":
    from airflow.utils.state import State  # noqa F401

    dag.clear()
    dag.run()
