import kubernetes
import os
import textwrap
import yaml
from time import sleep
from typing import List
import logging
from datetime import datetime
from watchers import ThreadedKuebrnetesLogReader, ThreadedKubernetesNamespaceWatcher

logging.basicConfig(level="INFO")

CUR_DIRECTORY = os.path.abspath(os.path.dirname(__file__))


def load_raw_formatted_file(fpath):
    text = ""
    with open(fpath, "r", encoding="utf-8") as src:
        text = src.read()
    return text


def create_pod_v1_object(pod_name, pod_image, command) -> kubernetes.client.V1Pod:
    pod_yaml = load_raw_formatted_file(os.path.join(CUR_DIRECTORY, "pod.yaml"))
    pod_yaml = pod_yaml.format(pod_name="lama", pod_image="ubuntu")
    pod = yaml.safe_load(pod_yaml)
    pod["spec"]["containers"][0]["command"] = command
    return pod


# test kubernetes
kubernetes.config.load_kube_config()
contexts, active_context = kubernetes.config.list_kube_config_contexts()
current_namespace = active_context["context"]["namespace"]
client = kubernetes.client.CoreV1Api()

bash_script = """
echo "Starting"
while true; do
    echo $((`date +%s`*1000+`date +%-N`/1000000))
    sleep 1
done
"""

pod_to_execute = create_pod_v1_object("lama", "ubuntu", ["bash", "-c", bash_script])

ns_watcher = ThreadedKubernetesNamespaceWatcher(client, current_namespace)


def ns_watcher_updated(event):
    logging.info(event["type"])
    pass


ns_watcher.on("updated", ns_watcher_updated)
ns_watcher.start()

print("Validate the pod...")
try:
    status = client.read_namespaced_pod_status("lama", current_namespace)
except Exception:
    print("Pod dose not exist creating...")
    client.create_namespaced_pod(current_namespace, pod_to_execute)
    sleep(10)

print("Watch...")

log_reader = ThreadedKuebrnetesLogReader(client, "lama", current_namespace)

global started
started = None


def read_log_test(msg: str):
    global started
    try:
        for timestamp in msg.split("\n"):
            timestamp = timestamp.strip()
            if len(timestamp) == 0:
                continue
            dt = datetime.utcfromtimestamp(int(timestamp) / 1000)
            logging.info(f"Read timestamp: {dt}")
            if started is None:
                started = dt
            elif (dt - started).seconds > 20:
                logging.info("Stopped...")
                log_reader.stop()
    except Exception as e:
        logging.warn(e)
        logging.warn("Could not read log message: " + msg)


log_reader.on("log", read_log_test)
log_reader.start()
log_reader.join()
logging.info("Stopping namespace watcher..")
ns_watcher.stop()
