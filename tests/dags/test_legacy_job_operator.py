from utils import default_args, name_from_file
from airflow import DAG
from airflow_kubernetes_job_operator.kubernetes_legacy_job_operator import KubernetesLegacyJobOperator

dag = DAG(
    name_from_file(__file__),
    default_args=default_args,
    description="Test base job operator",
    schedule_interval=None,
    catchup=False,
)


bash_script = """
#/usr/bin/env bash
echo "Starting"
TIC_COUNT=10
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

op = KubernetesLegacyJobOperator(
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

if __name__ == "__main__":
    dag.clear(reset_dag_runs=True)
    dag.run()
