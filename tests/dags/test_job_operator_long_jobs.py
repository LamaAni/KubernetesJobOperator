from datetime import timedelta
from utils import resolve_file, default_args
from airflow import DAG
from airflow_kubernetes_job_operator.kubernetes_job_operator import KubernetesJobOperator

dag = DAG(
    "kub-job-op-long",
    default_args=default_args,
    description="Test base job operator",
    schedule_interval=None,
    catchup=False,
)

envs = {
    "PASS_ARG": "a test",
}

total_time_seconds = round(timedelta(hours=8).total_seconds())

KubernetesJobOperator(
    task_id="test-long-job-success",
    body_filepath=resolve_file("./templates/test_long_job.yaml"),
    envs={
        "PASS_ARG": "a long test",
        "TIC_COUNT": str(total_time_seconds),
    },
    dag=dag,
)


if __name__ == "__main__":
    dag.clear(reset_dag_runs=True)
    dag.run()
