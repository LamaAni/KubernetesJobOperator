from tests.utils import logging, load_yaml_objects
from typing import Type
from airflow_kubernetes_job_operator.kube_api import (
    KubeApiRestClient,
    CreateNamespaceResource,
    DeleteNamespaceResource,
    ConfigureNamespaceResource,
    NamespaceWatchQuery,
)


objs = load_yaml_objects("./client/test_service.yaml")


client = KubeApiRestClient()
watcher = NamespaceWatchQuery()


def create_queries(action: Type[ConfigureNamespaceResource]):
    queries = [action(o, namespace=client.get_default_namespace()) for o in objs]
    for q in queries:
        q.pipe_to_logger(logging)
    return queries


client.query_async(watcher)
watcher.wait_until_running()
logging.info("Watcher is running, creating")
client.query(create_queries(CreateNamespaceResource))
logging.info("All created.")
client.query(create_queries(DeleteNamespaceResource))
logging.info("All deleted")
watcher.stop()
