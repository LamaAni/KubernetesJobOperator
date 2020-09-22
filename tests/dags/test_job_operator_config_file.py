# import os
# from airflow import DAG
# from airflow_kubernetes_job_operator.kubernetes_job_operator import KubernetesJobOperator
# from airflow.utils.dates import days_ago

# default_args = {"owner": "tester", "start_date": days_ago(2), "retries": 0}
# dag = DAG(
#     "job-operator-config-file", default_args=default_args, description="Test base job operator", schedule_interval=None,
# )

# job_task = KubernetesJobOperator(
#     task_id="test-job",
#     dag=dag,
#     image="ubuntu",
#     in_cluster=False,
#     cluster_context="docker-desktop",
#     config_file=os.path.expanduser("~/.kube/config_special"),
#     command=["bash", "-c", 'echo "all ok"'],
# )
