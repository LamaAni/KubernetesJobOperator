from utils import default_args
from datetime import timedelta
from airflow import DAG
from airflow_kubernetes_job_operator import (
    KubernetesJobOperator,
    JobRunnerDeletePolicy,
    KubernetesLegacyJobOperator,
)

dag = DAG(
    "kub-job-op-test-jinja",
    default_args=default_args,
    description="Test base job operator",
    schedule_interval=None,
    catchup=False,
)

with dag:
    namespace = None
    default_delete_policy = JobRunnerDeletePolicy.Never

    KubernetesJobOperator(
        task_id="test-job-success",
        namespace=namespace,
        body_filepath="./templates/test_job_with_service.yaml",
        delete_policy=default_delete_policy,
    )

if __name__ == "__main__":
    dag.clear(reset_dag_runs=True)
    dag.run()
