import kubernetes
import os
import yaml
from utils import logging
from src.watchers.threaded_kubernetes_watch import ThreadedKubernetesWatch

logging.basicConfig(level="INFO")
CUR_DIRECTORY = os.path.abspath(os.path.dirname(__file__))


# load kubernetes configuration.
kubernetes.config.load_kube_config()
contexts, active_context = kubernetes.config.list_kube_config_contexts()
current_namespace = active_context["context"]["namespace"]

client = kubernetes.client.CoreV1Api()


def list_namespace_pods():
    info = client.read_namespaced_pod_log_with_http_info(
        name="tester",
        namespace=current_namespace,
        follow=True,
        _preload_content=False,
        _request_timeout=5,
    )
    return info[0]


watcher = ThreadedKubernetesWatch(list_namespace_pods, read_as_object=False)

for event in watcher.stream():
    logging.info(yaml.dump(event))
    # watcher.stop()
