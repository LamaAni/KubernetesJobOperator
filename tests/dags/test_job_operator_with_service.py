from utils import default_args, name_from_file
from airflow import DAG
from airflow_kubernetes_job_operator import (
    KubernetesJobOperator,
    JobRunnerDeletePolicy,
)

dag = DAG(
    name_from_file(__file__),
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
    dag.clear()
    dag.run()
