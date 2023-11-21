from utils import default_args, name_from_file
from airflow import DAG
from airflow_kubernetes_job_operator.kubernetes_job_operator import KubernetesJobOperator


dag = DAG(
    name_from_file(__file__),
    default_args=default_args,
    description="Test base job operator",
    schedule_interval=None,
    catchup=False,
)

with dag:
    KubernetesJobOperator(task_id="test_dbl_log", body_filepath=__file__ + ".yaml")

if __name__ == "__main__":
    from tests.test_utils import test_dag
    test_dag(dag)
