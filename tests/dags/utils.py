import os
from airflow_kubernetes_job_operator.kube_api import KubeApiConfiguration

# import airflow.configuration
import warnings
import logging
import sys

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
