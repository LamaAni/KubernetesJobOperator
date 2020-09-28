import os
from airflow_kubernetes_job_operator.kube_api import KubeApiConfiguration, KubeObjectKind

# import airflow.configuration
import warnings
import logging
import sys

logging.basicConfig(level=logging.INFO)

KubeApiConfiguration.register_kind(
    name="HCJob",
    api_version="hc.dto.cbsinteractive.com/v1alpha1",
    parse_kind_state=KubeObjectKind.parse_state_job,
)

# TODO:
# DONE 1. move register_global_kind to KubeApiConfiguration
# 2. figure out why load_kubernetes_configuration is not giving the right error.
#   The error possibly came from get default namespace.
# DONE 3. Add ability to ignore invalid kinds in the runner if the kind is not the main kind.
# DONE 4. Add cluster info to the execution log.
# DONE 5. Check error in pod execution. (error in configuration)

KubeApiConfiguration.add_kube_config_search_location("~/composer_kube_config")  # second
KubeApiConfiguration.add_kube_config_search_location("~/gcs/dags/config/hcjobs-kubeconfig.yaml")  # first
KubeApiConfiguration.set_default_namespace("cdm-hcjobs")
config = None
config_host = None
config_filepath = None


try:
    config = KubeApiConfiguration.load_kubernetes_configuration()
    assert config is not None
    config_host = config.host
    config_filepath = config.filepath

    print_version = str(sys.version).replace("\n", " ")
    logging.info(
        f"""
    -----------------------------------------------------------------------
    Context: {KubeApiConfiguration.get_active_context_info(config)}
    home directory: {os.path.expanduser('~')}
    Config host: {config.host} 
    Config filepath: {config.filepath}
    Default namespace: {KubeApiConfiguration.get_default_namespace(config)}
    Executing dags in python version: {print_version}
    -----------------------------------------------------------------------
    """
    )
except Exception as ex:
    logging.error(
        """
-----------------------------------------------------------------------
Failed to retrive config, kube config could not be loaded.
----------------------------------------------------------------------
""",
        ex,
    )

warnings.filterwarnings("ignore", category=DeprecationWarning)

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
