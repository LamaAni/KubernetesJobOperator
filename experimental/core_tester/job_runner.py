import kubernetes
import os
import yaml
from utils import logging, load_raw_formatted_file
from datetime import datetime
from watchers import ThreadedKubernetesNamespaceObjectsWatcher

logging.basicConfig(level="INFO")

CUR_DIRECTORY = os.path.abspath(os.path.dirname(__file__))


def create_job(job_name, pod_image, command) -> kubernetes.client.V1Pod:
    job_yaml = load_raw_formatted_file(os.path.join(CUR_DIRECTORY, "job.yaml"))
    job_yaml = job_yaml.format(job_name=job_name, pod_image=pod_image)
    pod = yaml.safe_load(job_yaml)
    pod["spec"]["template"]["spec"]["containers"][0]["command"] = command
    return pod


# test kubernetes
kubernetes.config.load_kube_config()
contexts, active_context = kubernetes.config.list_kube_config_contexts()
current_namespace = active_context["context"]["namespace"]
client = kubernetes.client.CoreV1Api()
batchClient = kubernetes.client.BatchV1Api()

bash_script = load_raw_formatted_file(os.path.join(CUR_DIRECTORY, "pod_script.sh"))


def read_pod_log(msg: str, sender):
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
ko_watcher.on("log", read_pod_log)
ko_watcher.on("status", object_status_changed)

ko_watcher.watch_namespace(current_namespace)
print("Executing the job")
job_id = "lama"


try:
    status = batchClient.read_namespaced_job_status("lama", current_namespace)
except Exception as e:
    print(f"Job does not exist {e.reason}. creating...")
    job_to_execute = create_job(job_id, "ubuntu", ["bash", "-c", bash_script])
    try:
        batchClient.create_namespaced_job(current_namespace, job_to_execute)
    finally:
        logging.warning("Stopped watcher since due to errors in job create.")
        ko_watcher.stop()

    print("Waiting for job to run..")
    ko_watcher.waitfor_status(
        kind="Job",
        name="lama",
        namespace=current_namespace,
        predict=lambda status, sender: status != "Pending",
    )

print("Watch...")

job_watch_object = ko_watcher.waitfor_status(
    kind="Job",
    name="lama",
    namespace=current_namespace,
    predict=lambda status, sender: status != "Running",
)

logging.info("Stopping namespace watcher..")
ko_watcher.stop()
