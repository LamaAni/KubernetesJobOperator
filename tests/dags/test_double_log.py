import os
from utils import default_args, resolve_file
from airflow import DAG
from airflow_kubernetes_job_operator.kubernetes_job_operator import KubernetesJobOperator


dag = DAG(
    "kub-job-op-custom",
    default_args=default_args,
    description="Test base job operator",
    schedule_interval=None,
    catchup=False,
)

with dag:
    KubernetesJobOperator(task_id="test_dbl_log", body_filepath=__file__ + ".yaml")

if __name__ == "__main__":
    dag.clear()
    dag.run()
