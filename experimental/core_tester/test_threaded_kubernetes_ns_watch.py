import kubernetes
import os
from utils import logging
from airflow_kubernetes_job_operator.threaded_kubernetes_resource_watchers import (
    ThreadedKubernetesNamespaceResourcesWatcher,
)

logging.basicConfig(level="INFO")
CUR_DIRECTORY = os.path.abspath(os.path.dirname(__file__))


# load kubernetes configuration.
kubernetes.config.load_kube_config()
contexts, active_context = kubernetes.config.list_kube_config_contexts()
current_namespace = active_context["context"]["namespace"]

client = kubernetes.client.CoreV1Api()

watcher = ThreadedKubernetesNamespaceResourcesWatcher(client)


def resource_status_changed(status, sender):
    logging.info(sender.id + ":" + status)
    pass


def on_resource_error(err, sender):
    logging.error(f"{err}")


watcher.on("status", resource_status_changed)
watcher.on("error", lambda err, sender: on_resource_error(err, sender))
watcher.watch_namespace(current_namespace)

# Will wait forever
watcher.waitfor_status(kind="Pod", name="this-pod-will-end-the-test", status="Pending")
