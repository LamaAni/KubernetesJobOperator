from utils import resolve_file, default_args
from datetime import timedelta
from airflow import DAG
from airflow_kubernetes_job_operator.kubernetes_job_operator import KubernetesJobOperator

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
}

tj_success = KubernetesJobOperator(
    task_id="test-job-success",
    namespace=namespace,
    body_filepath=resolve_file(__file__ + ".success.yaml"),
    envs=envs,
    dag=dag,
)
tj_fail = KubernetesJobOperator(
    task_id="test-job-fail",
    namespace=namespace,
    body_filepath=resolve_file(__file__ + ".fail.yaml"),
    envs=envs,
    dag=dag,
)
tj_pod_fail = KubernetesJobOperator(
    task_id="test-pod-fail",
    namespace=namespace,
    body_filepath=resolve_file(__file__ + ".pod.fail.yaml"),
    envs=envs,
    dag=dag,
)
tj_overrides = KubernetesJobOperator(
    task_id="test-job-overrides",
    dag=dag,
    namespace=namespace,
    image="ubuntu",
    envs=envs,
    command=["bash", "-c", 'echo "Starting $PASS_ARG"; sleep 10; echo end'],
)
ti_timeout = KubernetesJobOperator(
    task_id="test-job-timeout",
    namespace=namespace,
    body_filepath=resolve_file(__file__ + ".success.yaml"),
    envs={
        "TIC_COUNT": "100",
        "PASS_ARG": "timeout",
    },
    dag=dag,
    execution_timeout=timedelta(seconds=3),
)

if __name__ == "__main__":
    dag.clear(reset_dag_runs=True)
    dag.run()
