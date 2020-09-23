from utils import resolve_file, default_args
from airflow import DAG
from airflow_kubernetes_job_operator.kubernetes_job_operator import KubernetesJobOperator

dag = DAG(
    "bjo-long",
    default_args=default_args,
    description="Test base job operator",
    schedule_interval=None,
    catchup=False,
)

envs = {
    "PASS_ARG": "a test",
}

tj_success = KubernetesJobOperator(
    task_id="test-long-job-success",
    body_filepath=resolve_file(__file__ + ".long.yaml"),
    envs={
        "PASS_ARG": "a long test",
        "TIC_COUNT": "30",
    },
    dag=dag,
)


if __name__ == "__main__":
    dag.clear(reset_dag_runs=True)
    dag.run()
