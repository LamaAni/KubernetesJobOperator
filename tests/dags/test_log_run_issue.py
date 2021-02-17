import datetime as dt
from typing import Union

import pendulum
from airflow import DAG
from airflow_kubernetes_job_operator.kubernetes_job_operator import JobRunnerDeletePolicy, KubernetesJobOperator


def create_dag(
    dag_name: str,
    environment: str,
    start_date: dt.datetime,
    schedule_interval: Union[dt.datetime, str],
) -> DAG:
    default_args = {
        "owner": "Airflow",
        "start_date": start_date,
        "retries": 0,
    }
    dag = DAG(
        f"{dag_name}_{environment}",
        default_args=default_args,
        description="Test a 'connection reset by peer' issue with K8S jobs.",
        schedule_interval=schedule_interval,
        max_active_runs=1,
    )

    k8s_task = KubernetesJobOperator(
        task_id=f"{dag_name}_task",
        dag=dag,
        body_filepath="./test_log_run_issue.yaml",
        # config_file="<my-cluster-config>",
        # namespace="<my-namespace>",
        in_cluster=False,
        delete_policy=JobRunnerDeletePolicy.IfSucceeded,
    )

    dag >> k8s_task

    return dag


k8s_dag = create_dag(
    dag_name="test_connection_reset",
    environment="dev",
    start_date=dt.datetime(2021, 2, 15, tzinfo=pendulum.timezone("Europe/Brussels")),
    schedule_interval=None,
)

if __name__ == "__main__":
    k8s_dag.clear(reset_dag_runs=True)
    k8s_dag.run()
