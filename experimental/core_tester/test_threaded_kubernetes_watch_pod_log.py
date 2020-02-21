import kubernetes
import os
from .utils import logging
from airflow_kubernetes_job_operator.threaded_kubernetes_watch import (
    ThreadedKubernetesWatchPodLog,
    ThreadedKubernetesWatchNamspeace,
)

logging.basicConfig(level="INFO")
CUR_DIRECTORY = os.path.abspath(os.path.dirname(__file__))


# load kubernetes configuration.
kubernetes.config.load_kube_config()
contexts, active_context = kubernetes.config.list_kube_config_contexts()
current_namespace = active_context["context"]["namespace"]

client = kubernetes.client.CoreV1Api()

namsepaceWatcher = ThreadedKubernetesWatchNamspeace()
namsepaceWatcher.on("error", lambda err: logging.warning(err))
namsepaceWatcher.on("warning", lambda warning: logging.warning(warning))
namsepaceWatcher.on("data", lambda event: logging.info(event["type"]))
namsepaceWatcher.start(client=client, namespace=current_namespace, kind="Pod")

watcher = ThreadedKubernetesWatchPodLog()

watcher.on("error", lambda err: logging.warning(err))
watcher.on("warning", lambda warning: logging.warning(warning))

try:
    for msg in watcher.stream(client=client, name="tester", namespace=current_namespace):
        logging.info(msg)
finally:
    logging.info("Stopping the namespace reader...")
    namsepaceWatcher.stop()
