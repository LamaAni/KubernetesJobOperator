import os
from airflow_kubernetes_job_operator.kube_api import KubeApiConfiguration, KubeResourceKind

# import airflow.configuration
import warnings
import logging
import sys

KubeApiConfiguration.add_kube_config_search_location("~/composer_kube_config")  # second
KubeApiConfiguration.add_kube_config_search_location("~/gcs/dags/config/hcjobs-kubeconfig.yaml")  # first
KubeApiConfiguration.set_default_namespace("cdm-hcjobs")

KubeApiConfiguration.register_kind(
    name="HCJob",
    api_version="hc.dto.cbsinteractive.com/v1alpha1",
    parse_kind_state=KubeResourceKind.parse_state_job,
)


logging.basicConfig(level=logging.INFO)
warnings.filterwarnings("ignore", category=DeprecationWarning)


def print_default_kube_configuration():
    try:
        config = KubeApiConfiguration.load_kubernetes_configuration()
        assert config is not None

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


print_default_kube_configuration()

default_args = {"owner": "tester", "start_date": "1/1/2020", "retries": 0}

DAGS_PATH = os.path.dirname(__file__)


def resolve_file(fpath: str):
    if fpath.startswith("."):
        if fpath.startswith("./"):
            fpath = os.path.join(DAGS_PATH, fpath[2:])
        else:
            fpath = os.path.join(DAGS_PATH, fpath)
    return os.path.abspath(fpath)
