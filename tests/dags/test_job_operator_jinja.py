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
    user_defined_macros={
        "test_macro": lambda a: f"my {a}",
        "default_image": "ubuntu",
    },
)

namespace = None

envs = {
    "TIC_COUNT": 3,
    "PASS_ARG": "a test",
    "JINJA_ENV": "{{ ds }}",
}

default_delete_policy = JobRunnerDeletePolicy.Never

KubernetesJobOperator(
    task_id="test-job-success",
    namespace=namespace,
    image="{{default_image}}",
    body_filepath="./templates/test_job.success.jinja.yaml",
    envs=envs,
    dag=dag,
    delete_policy=default_delete_policy,
    jinja_job_args={"test": "lama"},
)

if __name__ == "__main__":
    dag.clear()
    dag.run()
