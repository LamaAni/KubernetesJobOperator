from utils import resolve_file, default_args
from airflow import DAG
from airflow_kubernetes_job_operator.kubernetes_job_operator import KubernetesJobOperator


dag = DAG(
    "bjo-custom",
    default_args=default_args,
    description="Test base job operator",
    schedule_interval=None,
    catchup=False,
)

envs = {
    "PASS_ARG": "a test",
}

KubernetesJobOperator(
    task_id="test-job-custom-success",
    body_filepath=resolve_file("./.local/test_custom.yaml"),
    envs=envs,
    dag=dag,
)

KubernetesJobOperator(
    task_id="test-job-custom-fail",
    body_filepath=resolve_file("./.local/test_custom.fail.yaml"),
    envs=envs,
    dag=dag,
)


if __name__ == "__main__":
    dag.clear(reset_dag_runs=True)
    dag.run()
