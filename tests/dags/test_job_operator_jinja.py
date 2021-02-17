from utils import default_args
from datetime import timedelta
from airflow import DAG
from airflow_kubernetes_job_operator import (
    KubernetesJobOperator,
    JobRunnerDeletePolicy,
    KubernetesLegacyJobOperator,
)

dag = DAG(
    "kub-job-op-test-jinja",
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

# bash_script = """
# #/usr/bin/env bash
# echo "Legacy start for taskid {{ti.task_id}} {{job.test}}"
# cur_count=0
# while true; do
#     cur_count=$((cur_count + 1))
#     if [ "$cur_count" -ge "$TIC_COUNT" ]; then
#         break
#     fi
#     date
#     sleep 1
# done

# echo "Complete"
# """
# KubernetesLegacyJobOperator(
#     task_id="legacy-test-job-success",
#     image="{{default_image}}",
#     cmds=["bash", "-c", bash_script],
#     dag=dag,
#     is_delete_operator_pod=True,
#     env_vars=envs,
#     delete_policy=default_delete_policy,
# )

if __name__ == "__main__":
    dag.clear(reset_dag_runs=True)
    dag.run()
