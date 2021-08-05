from utils import default_args, name_from_file
import kubernetes.client as k8s
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
with dag:
    KubernetesLegacyJobOperator(
        task_id="legacy-test-job-success",
        image="ubuntu",
        cmds=["bash", "-c", bash_script],
        is_delete_operator_pod=True,
    )

    KubernetesLegacyJobOperator(
        task_id="legacy-test-job-success-from-file",
        pod_template_file=__file__ + ".yaml",
        image="ubuntu",
        cmds=["sleep", "10"],
        is_delete_operator_pod=True,
    )

    KubernetesLegacyJobOperator(
        task_id="legacy-test-job-success-full-spec",
        image="ubuntu",
        full_pod_spec=k8s.V1Pod(
            metadata=k8s.V1ObjectMeta(
                name="lama",
                namespace="zav-dev",
            ),
            spec=k8s.V1PodSpec(
                containers=[
                    k8s.V1Container(
                        name="lama",
                        image="ubuntu",
                        command=[
                            "bash",
                            "-c",
                            """
    echo "sleeping"
    sleep 3
    echo "ok"
    """,
                        ],
                    )
                ]
            ),
        ),
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
