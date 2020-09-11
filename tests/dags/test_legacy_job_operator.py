from airflow import DAG
from airflow_kubernetes_job_operator.kubernetes_legacy_job_operator import KubernetesLegacyJobOperator

# from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {"owner": "tester", "start_date": days_ago(2), "retries": 0}

dag = DAG(
    "legacy-bjo",
    default_args=default_args,
    description="Test base job operator",
    schedule_interval=None,
)

bash_script = """
#/usr/bin/env bash
echo "Starting"
TIC_COUNT=0
cur_count=0
while true; do
    cur_count=$((cur_count + 1))
    if [ "$cur_count" -ge "$TIC_COUNT" ]; then
        break
    fi
    date
    sleep 1
done

echo "Complete"
"""
# BashOperator(bash_command="date", task_id="test-bash", dag=dag)

KubernetesLegacyJobOperator(
    task_id="legacy-test-job-success",
    image="ubuntu",
    cmds=["bash", "-c", bash_script],
    dag=dag,
    is_delete_operator_pod=True,
)

KubernetesLegacyJobOperator(
    task_id="legacy-test-job-fail",
    image="ubuntu",
    cmds=["bash", "-c", bash_script + "\nexit 99"],
    dag=dag,
    is_delete_operator_pod=True,
)

