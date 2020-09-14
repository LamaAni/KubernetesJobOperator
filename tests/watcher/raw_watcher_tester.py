import kubernetes
import os
import yaml
from tests.watcher.utils import logging
from airflow_kubernetes_job_operator.kube_api.api_watchers import KubeApiNamespaceWatcher, KubeApiNamespaceWatcherKind

logging.basicConfig(level="INFO")

# load kubernetes configuration.
kubernetes.config.load_kube_config()
contexts, active_context = kubernetes.config.list_kube_config_contexts()
current_namespace = active_context["context"].get("namespace", "default")

client = kubernetes.client.CoreV1Api()


def handle_error(watcher, err):
    logging.error(err)
    watcher.stop()


watcher = KubeApiNamespaceWatcher(current_namespace, kind=KubeApiNamespaceWatcherKind.Event)
watcher.on("error", handle_error)
watcher.start()
if watcher.is_running:
    logging.info("Starting watch...")
    for ev in watcher.stream("api_event"):
        logging.info(ev)
