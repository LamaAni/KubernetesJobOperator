import kubernetes
import os
import yaml
from utils import logging, load_raw_formatted_file
from datetime import datetime
from watchers.threaded_kubernetes_object_watchers import (
    ThreadedKubernetesNamespaceObjectsWatcher,
)

logging.basicConfig(level="INFO")

CUR_DIRECTORY = os.path.abspath(os.path.dirname(__file__))


def create_pod_v1_object(pod_name, pod_image, command) -> kubernetes.client.V1Pod:
    pod_yaml = load_raw_formatted_file(os.path.join(CUR_DIRECTORY, "pod.yaml"))
    pod_yaml = pod_yaml.format(pod_name=pod_name, pod_image=pod_image)
    pod = yaml.safe_load(pod_yaml)
    pod["spec"]["containers"][0]["command"] = command
    return pod


# test kubernetes
kubernetes.config.load_kube_config()
contexts, active_context = kubernetes.config.list_kube_config_contexts()
current_namespace = active_context["context"]["namespace"]
client = kubernetes.client.CoreV1Api()

bash_script = load_raw_formatted_file(os.path.join(CUR_DIRECTORY, "pod_script.sh"))

pod_to_execute = create_pod_v1_object("lama", "ubuntu", ["bash", "-c", bash_script])


def read_log_test(msg: str, sender):
    for msg_part in msg.split("\n"):
        msg_part = msg_part.strip()
        if len(msg_part) == 0:
            continue
        try:
            dt = datetime.utcfromtimestamp(int(msg_part) / 1000)
            logging.info(f"Read timestamp: {dt}")
        except Exception:
            logging.info(f"log: {msg_part}")


def object_status_changed(status, sender):
    logging.info(sender.id + ":" + status)
    pass


ko_watcher = ThreadedKubernetesNamespaceObjectsWatcher(client)
ko_watcher.on("log", read_log_test)
ko_watcher.on("status", object_status_changed)

ko_watcher.watch_namespace(current_namespace)
print("Validate the pod...")

try:
    status = client.read_namespaced_pod_status("lama", current_namespace)
except Exception:
    print("Pod dose not exist creating...")
    client.create_namespaced_pod(current_namespace, pod_to_execute)
    ko_watcher.waitfor_status(
        kind="Pod", name="lama", namespace=current_namespace, status="Running"
    )
    print("Pod is running...")

print("Watch...")

ko_watcher.waitfor_status(
    kind="Pod",
    name="lama",
    namespace=current_namespace,
    predict=lambda status, sender: status != "Running",
)

logging.info("Stopping namespace watcher..")
ko_watcher.stop()
