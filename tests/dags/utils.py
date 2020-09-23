import os
from airflow_kubernetes_job_operator.kube_api.config import DEFAULT_KUBE_CONFIG_LOCATIONS
from airflow_kubernetes_job_operator.kube_api import KubeApiConfiguration

# import airflow.configuration
import warnings
import logging
import sys

logging.basicConfig(level=logging.INFO)
DEFAULT_KUBE_CONFIG_LOCATIONS.append("/home/airflow/.kube/config")

config = None
config_host = None
config_filepath = None


config = KubeApiConfiguration.load_kubernetes_configuration_from_file()
assert config is not None
config_host = config.host
config_filepath = config.filepath


# pull_execution_cluster_config()

print_version = str(sys.version).replace("\n", " ")
logging.info(
    f"""
-----------------------------------------------------------------------
home directory: {os.path.expanduser('~')}
Config host: {config.host} 
Config filepath: {config.filepath}
Default namespace: {KubeApiConfiguration.get_default_namespace(config)}
Executing dags in python version: {print_version}
-----------------------------------------------------------------------
"""
)

warnings.filterwarnings("ignore", category=DeprecationWarning)

# KubeObjectKind.register_global_kind(
#     KubeObjectKind("HCJob", "hc.dto.cbsinteractive.com/v1alpha1", parse_kind_state=KubeObjectKind.parse_state_job)
# )


default_args = {"owner": "tester", "start_date": "1/1/2020", "retries": 0}

REPO_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
DAGS_PATH = os.path.join(REPO_PATH, "tests", "dags")


def resolve_file(fpath: str):
    if fpath.startswith("."):
        if fpath.startswith("./"):
            fpath = os.path.join(DAGS_PATH, fpath[2:])
        else:
            fpath = os.path.join(DAGS_PATH, fpath)
    return os.path.abspath(fpath)
