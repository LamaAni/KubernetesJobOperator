from utils import default_args
from datetime import timedelta
from airflow import DAG
from airflow_kubernetes_job_operator.kubernetes_job_operator import KubernetesJobOperator, JobRunnerDeletePolicy

dag = DAG(
    "kub-job-op",
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

default_delete_policy = JobRunnerDeletePolicy.IfSucceeded

# Job
KubernetesJobOperator(
    task_id="test-job-success",
    namespace=namespace,
    body_filepath="./templates/test_job.success.yaml",
    envs=envs,
    dag=dag,
    delete_policy=default_delete_policy,
)
KubernetesJobOperator(
    task_id="test-job-fail",
    namespace=namespace,
    body_filepath="./templates/test_job.fail.yaml",
    envs=envs,
    dag=dag,
    delete_policy=default_delete_policy,
)

# Pod
KubernetesJobOperator(
    task_id="test-pod-fail",
    namespace=namespace,
    body_filepath="./templates/test_pod.fail.yaml",
    envs=envs,
    dag=dag,
    delete_policy=default_delete_policy,
)
KubernetesJobOperator(
    task_id="test-pod-fail",
    namespace=namespace,
    body_filepath="./templates/test_pod.success.yaml",
    envs=envs,
    dag=dag,
    delete_policy=default_delete_policy,
)

# -- others.
KubernetesJobOperator(
    task_id="test-job-overrides",
    dag=dag,
    namespace=namespace,
    image="ubuntu",
    envs=envs,
    command=["bash", "-c", 'echo "Starting $PASS_ARG"; sleep 10; echo end'],
    delete_policy=default_delete_policy,
)

KubernetesJobOperator(
    task_id="test-job-timeout",
    namespace=namespace,
    body_filepath="./templates/test_job.success.yaml",
    envs={
        "TIC_COUNT": "100",
        "PASS_ARG": "timeout",
    },
    dag=dag,
    execution_timeout=timedelta(seconds=3),
    delete_policy=default_delete_policy,
)


if __name__ == "__main__":
    dag.clear(reset_dag_runs=True)
    dag.run()
