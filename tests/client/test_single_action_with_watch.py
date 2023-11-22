import os
import yaml
import kubernetes
from uuid import uuid1
from tests.utils import logging
from airflow_kubernetes_job_operator.kube_api import (
    KubeApiRestClient,
    NamespaceWatchQuery,
    CreateNamespaceResource,
    DeleteNamespaceResource,
    KubeResourceDescriptor,
    KubeResourceState,
    KubeResourceKind,
)


kubernetes.client.CoreV1Api().delete_namespaced_service

KubeResourceKind.register_global_kind(
    KubeResourceKind(
        "HCjob",
        "hc.dto.cbsinteractive.com/v1alpha1",
        parse_kind_state=KubeResourceKind.parse_state_job,
    )
)


def load_yaml_obj_configs(fpath: str):
    if fpath.startswith("./") or fpath.startswith("../"):
        fpath = os.path.join(os.path.dirname(__file__), fpath)
    fpath = os.path.abspath(fpath)
    assert os.path.isfile(fpath), Exception(f"{fpath} is not a file or dose not exist")
    col = []
    with open(fpath, "r") as raw:
        col = list(yaml.safe_load_all(raw.read()))
    return col


client = KubeApiRestClient()
namespace = client.get_default_namespace()
task_id = str(uuid1())
label_selector = None
label_selector = f"task-id={task_id}"

obj_definitions = load_yaml_obj_configs("../../.local/test_custom.yaml")
task = KubeResourceDescriptor(obj_definitions[0])
obj: dict = None
kinds = set()


def apply_on_objects(operator):
    queries = []
    for obj in obj_definitions:
        update_metadata_labels(obj, {"task-id": task_id})
        q = operator(obj, namespace=namespace)
        q.pipe_to_logger(logging)
        queries.append(q)
    return queries


def update_metadata_labels(obj: dict, labels: dict):
    for v in list(obj.values()):
        if isinstance(v, dict):
            update_metadata_labels(v, labels)
    if "spec" in obj:
        obj["metadata"] = obj.get("metadata", {})
        obj["metadata"]["labels"] = obj["metadata"].get("labels", {})
        obj["metadata"]["labels"].update(labels)


watcher = NamespaceWatchQuery(namespace=namespace, label_selector=label_selector)
watcher.pipe_to_logger(logging)
client.query_async(watcher)
watcher.wait_until_running()
logging.info(
    f"Watcher started for {task_id}, watch kinds: " + ", ".join(watcher.kinds.keys())
)

logging.info(f"Sending {task} to server with dependencies...")
client.query_async(apply_on_objects(CreateNamespaceResource))

logging.info(f"Waiting for {task} of kind {task.kind.name} to succeed or fail ...")
final_state = watcher.wait_for_state(
    [KubeResourceState.Failed, KubeResourceState.Succeeded],
    kind=task.kind,
    namespace=task.namespace,
    name=task.name,
)

logging.info(f"Finalized state with: {final_state}")
logging.info("Deleting leftovers..")
client.query(apply_on_objects(DeleteNamespaceResource))
client.stop()
