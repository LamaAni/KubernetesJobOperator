import kubernetes
import os
import yaml
from utils import logging
from airflow_kubernetes_job_operator.threaded_kubernetes_watch import ThreadedKubernetesWatchNamspeace

logging.basicConfig(level="INFO")
CUR_DIRECTORY = os.path.abspath(os.path.dirname(__file__))


# load kubernetes configuration.
kubernetes.config.load_kube_config()
contexts, active_context = kubernetes.config.list_kube_config_contexts()
current_namespace = active_context["context"]["namespace"]

client = kubernetes.client.CoreV1Api()

watcher = ThreadedKubernetesWatchNamspeace()

for event in watcher.stream(client, kind="Pod", namespace=current_namespace):
    name = event["object"]["metadata"]["name"]
    event_type = event["type"]
    logging.info(f"{name}: {event_type}")
    # watcher.stop()
