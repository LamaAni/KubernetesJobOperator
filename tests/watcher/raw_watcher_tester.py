import kubernetes
import os
import yaml

import dateutil.parser
from datetime import datetime
from tests.watcher.utils import logging
from airflow_kubernetes_job_operator.kube_api.watchers import KubeApiNamespaceWatcher, KubeApiNamespaceWatcherKind

logging.basicConfig(level="INFO")

# load kubernetes configuration.
kubernetes.config.load_kube_config()
contexts, active_context = kubernetes.config.list_kube_config_contexts()
current_namespace = active_context["context"].get("namespace", "default")

client = kubernetes.client.CoreV1Api()


def handle_error(watcher, err):
    logging.error(err)
    watcher.stop()


# Timeout for multiple reads is false. The query command show allow ~ 5 mins. Otherwise
# event updates will be reread.

watcher = KubeApiNamespaceWatcher(current_namespace, kind=KubeApiNamespaceWatcherKind.Event)

watcher.on("error", handle_error)
watcher.on("warning", lambda self, msg: logging.warning(msg))
watcher.start()
if watcher.is_running:
    logging.info("Starting watch...")
    for ev in watcher.stream("api_event"):
        logging.info(f'[{ev["type"]} {ev["object"]["kind"]}]: {ev["object"]["message"]}')
