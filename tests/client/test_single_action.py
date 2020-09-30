import os
import yaml
import kubernetes
import kubernetes.config.kube_config
from tests.utils import logging
from zthreading.tasks import Task
from airflow_kubernetes_job_operator.kube_api.client import KubeApiRestClient
from airflow_kubernetes_job_operator.kube_api import CreateNamespaceResource, DeleteNamespaceResource


def load_yaml_obj_configs(fpath: str):
    if fpath.startswith("./"):
        fpath = os.path.join(os.path.dirname(__file__), fpath)
    fpath = os.path.abspath(fpath)
    assert os.path.isfile(fpath), Exception(f"{fpath} is not a file or dose not exist")
    col = []
    with open(fpath, "r") as raw:
        col = list(yaml.safe_load_all(raw.read()))
    return col


client = KubeApiRestClient()

obj_definitions = load_yaml_obj_configs("./test_deployment.yaml")
obj: dict = None
queries = []

for obj in obj_definitions:
    q = DeleteNamespaceResource(obj, namespace=client.get_default_namespace())
    q.pipe_to_logger(logging)
    queries.append(q)

logging.info("Sending to server...")
rslt = client.query(queries)
