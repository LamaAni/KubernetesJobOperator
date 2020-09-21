import os
import yaml
import kubernetes
import kubernetes.config.kube_config
from tests.utils import logging
from zthreading.tasks import Task
from airflow_kubernetes_job_operator.kube_api import (
    KubeApiRestClient,
    NamespaceWatchQuery,
    CreateNamespaceObject,
    DeleteNamespaceObject,
    KubeObjectDescriptor,
    KubeObjectState,
)

from airflow_kubernetes_job_operator.utils import randomString


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
namespace = client.get_default_namespace()
task_id = randomString(10)

obj_definitions = load_yaml_obj_configs("./test_job.yaml")
task = KubeObjectDescriptor(obj_definitions[0])
obj: dict = None
queries = []
kinds = set()


def mark_metadata_labels(obj: dict, labels: dict):
    for v in list(obj.values()):
        if isinstance(v, dict):
            mark_metadata_labels(v, labels)
    if "spec" in obj:
        obj["metadata"] = obj.get("metadata", {})
        obj["metadata"]["labels"] = obj["metadata"].get("labels", {})
        obj["metadata"]["labels"].update(labels)


for obj in obj_definitions:
    mark_metadata_labels(obj, {"task-id": task_id})
    desc = KubeObjectDescriptor(obj)
    kinds.add(desc.kind.name)
    q = CreateNamespaceObject(obj, namespace=namespace)
    q.pipe_to_logger(logging)
    queries.append(q)


watcher = NamespaceWatchQuery(kinds=kinds, namespace=namespace, label_selector=f"task-id={task_id}")
watcher.pipe_to_logger(logging)
client.async_query(watcher)
watcher.wait_until_running()
logging.info(f"Watcher started on kinds: {kinds}")

logging.info(f"Sending {task} to server with dependencies...")
client.async_query(queries)

logging.info(f"Waiting for {task} of kind {task.kind.name} to succeed or fail ...")
final_state = watcher.wait_for_state(
    [KubeObjectState.Failed, KubeObjectState.Succeeded],
    kind=task.kind,
    namespace=task.namespace,
    name=task.name,
)

logging.info(f"Finalized state with: {final_state}")
