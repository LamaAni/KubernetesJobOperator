from utils import default_args, name_from_file
from airflow import DAG
from airflow_kubernetes_job_operator.kubernetes_job_operator import KubernetesJobOperator

dag = DAG(
    name_from_file(__file__),
    default_args=default_args,
    description="Test base job operator",
    schedule_interval=None,
    catchup=False,
)

namespace = None

envs = {
    "PASS_ARG": "a test",
    "JINJA_ENV": "{{ ds }}",
}

default_delete_policy = "IfSucceeded"

# Job
KubernetesJobOperator(
    task_id="test-pod-success",
    namespace=namespace,
    body_filepath="./templates/test_pod.success.yaml",
    envs=envs,
    dag=dag,
    delete_policy=default_delete_policy,
) >> KubernetesJobOperator(
    task_id="test-job-success",
    namespace=namespace,
    body_filepath="./templates/test_job.success.yaml",
    envs=envs,
    dag=dag,
    delete_policy=default_delete_policy,
)


if __name__ == "__main__":
    dag.test()
