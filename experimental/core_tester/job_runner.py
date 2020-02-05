import kubernetes
import os
import textwrap
import yaml
from time import sleep
from typing import List
import logging

logging.basicConfig(level="DEBUG")

ROOT_DIRECTORY = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))


def load_raw_formatted_file(fpath):
    text = ""
    with open(fpath, "r", encoding="utf-8") as src:
        text = src.read()
    return text


def create_pod_v1_object(pod_name, pod_image, command) -> kubernetes.client.V1Pod:
    pod_yaml = load_raw_formatted_file(os.path.join(ROOT_DIRECTORY, "src", "pod.yaml"))
    pod_yaml = pod_yaml.format(pod_name="lama", pod_image="ubuntu")
    pod = yaml.safe_load(pod_yaml)
    pod["spec"]["containers"][0]["command"] = command
    return pod


# test kubernetes
kubernetes.config.load_kube_config()
contexts, active_context = kubernetes.config.list_kube_config_contexts()
current_namespace = active_context["context"]["namespace"]
client = kubernetes.client.CoreV1Api()
watcher = kubernetes.watch.Watch()

bash_script = """
echo "Starting"
while true; do
    date
    sleep 1
done
"""

pod_to_execute = create_pod_v1_object("lama", "ubuntu", ["bash", "-c", bash_script])

client.create_namespaced_pod(current_namespace, pod_to_execute)
sleep(3)

for event in client.read_namespaced_pod_log("lama", current_namespace, follow=True):
    logging.log(event)

kubernetes.utils.create_from_yaml
