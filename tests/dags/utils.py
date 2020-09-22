import os
from airflow.utils.dates import days_ago
from airflow_kubernetes_job_operator.kube_api import KubeObjectKind
import warnings

warnings.filterwarnings("ignore", category=DeprecationWarning)

KubeObjectKind.register_global_kind(
    KubeObjectKind("HCJob", "hc.dto.cbsinteractive.com/v1alpha1", parse_kind_state=KubeObjectKind.parse_state_job)
)


default_args = {"owner": "tester", "start_date": days_ago(2), "retries": 0}

REPO_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
DAGS_PATH = os.path.join(REPO_PATH, "tests", "dags")


def resolve_file(fpath: str):
    if fpath.startswith("."):
        if fpath.startswith("./"):
            fpath = os.path.join(DAGS_PATH, fpath[2:])
        else:
            fpath = os.path.join(DAGS_PATH, fpath)
    return os.path.abspath(fpath)
