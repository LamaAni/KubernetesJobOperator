from tests.utils import logging, load_yaml_objects
from time import sleep
from typing import Type
from airflow_kubernetes_job_operator.kube_api import (
    KubeApiRestClient,
    CreateNamespaceObject,
    DeleteNamespaceObject,
    ConfigureNamespaceObject,
    NamespaceWatchQuery,
)


objs = load_yaml_objects("./client/test_service.yaml")


client = KubeApiRestClient()
watcher = NamespaceWatchQuery()


def create_queries(action: Type[ConfigureNamespaceObject]):
    queries = [action(o, namespace=client.get_default_namespace()) for o in objs]
    for q in queries:
        q.pipe_to_logger(logging)
    return queries


client.query_async(watcher)
watcher.wait_until_running()
logging.info("Watcher is running, creating")
client.query(create_queries(CreateNamespaceObject))
logging.info("All created.")
client.query(create_queries(DeleteNamespaceObject))
logging.info("All deleted")
watcher.stop()
