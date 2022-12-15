import kubernetes
import os
import yaml
from utils import logging, load_raw_formatted_file
from airflow_kubernetes_job_operator.job_runner import JobRunner

logging.basicConfig(level="INFO")
CUR_DIRECTORY = os.path.abspath(os.path.dirname(__file__))


def create_job(name, namespace, image, command) -> kubernetes.client.V1Pod:
    body = load_raw_formatted_file(os.path.join(CUR_DIRECTORY, "job.yaml"))
    body = body.format(name=name, namespace=namespace, image=image)
    pod = yaml.safe_load(body)
    pod["spec"]["template"]["spec"]["containers"][0]["command"] = command
    return pod


def read_pod_log(msg: str, sender):
    logging.info(f"{sender.id}: {msg}")


def resource_status_changed(status, sender):
    logging.info(f"{sender.id} ({status})")
    pass


# load kubernetes configuration.
config_file = "/home/zav/.kube/config_special"  # change to test a specific config file
current_context = "docker-desktop"  # Change to test a specific context.

# prepare the runner.
runner = JobRunner()
runner.load_kuberntes_configuration(config_file=config_file, context=current_context)
current_namespace = runner.get_current_namespace()
print("Current namespace: " + current_namespace)
runner.on("log", read_pod_log)
runner.on("status", resource_status_changed)

# prepare the job to execute.
bash_script = load_raw_formatted_file(os.path.join(CUR_DIRECTORY, "pod_script.sh"))
body = create_job("lama", current_namespace, "ubuntu", ["bash", "-c", bash_script])
body = runner.prepare_body(body, 5)

# executing the job.
info, watcher = runner.execute_job(body)

# printing the result.
logging.info("Job result: " + info.status)
