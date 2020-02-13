import kubernetes
import os
import yaml
from utils import logging, load_raw_formatted_file
from datetime import datetime
from src.job_runner import JobRunner

logging.basicConfig(level="INFO")
CUR_DIRECTORY = os.path.abspath(os.path.dirname(__file__))


def create_job(name, namespace, image, command) -> kubernetes.client.V1Pod:
    job_yaml = load_raw_formatted_file(os.path.join(CUR_DIRECTORY, "job.yaml"))
    job_yaml = job_yaml.format(name=name, namespace=namespace, image=image)
    pod = yaml.safe_load(job_yaml)
    pod["spec"]["template"]["spec"]["containers"][0]["command"] = command
    return pod


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


def resource_status_changed(status, sender):
    logging.info(sender.id + ":" + status)
    pass


# load kubernetes configuration.
kubernetes.config.load_kube_config()
contexts, active_context = kubernetes.config.list_kube_config_contexts()
current_namespace = active_context["context"]["namespace"]

# prepare the runner.
runner = JobRunner()
runner.on("log", read_pod_log)
runner.on("status", resource_status_changed)

# prepare the job to execute.
bash_script = load_raw_formatted_file(os.path.join(CUR_DIRECTORY, "pod_script.sh"))
job_yaml = create_job("lama", current_namespace, "ubuntu", ["bash", "-c", bash_script])
job_yaml = runner.prepare_job_yaml(job_yaml, 5)

# executing the job.
info, watcher = runner.execute_job(job_yaml)

# printing the result.
logging.info("Job result: " + info.status)
