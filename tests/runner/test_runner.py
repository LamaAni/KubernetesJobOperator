import os
import yaml
import re
from tests.utils import logging, style, load_default_kube_config
from airflow_kubernetes_job_operator.utils import resolve_relative_path
from airflow_kubernetes_job_operator.job_runner import JobRunner, JobRunnerDeletePolicy
from airflow_kubernetes_job_operator.kube_api import KubeResourceKind, KubeApiConfiguration

load_default_kube_config()

KubeApiConfiguration.register_kind(
    "HCJob", "hc.dto.cbsinteractive.com/v1alpha1", parse_kind_state=KubeResourceKind.parse_state_job
)


def _exec_test(fpath):
    fpath = resolve_relative_path(fpath)
    logging.info(style.CYAN(f"Testing {fpath}:"))
    body = None
    with open(fpath, "r") as raw:
        body = list(yaml.safe_load_all(raw.read()))
    test_name = re.sub(r"[^a-zA-Z0-9]", "-", os.path.splitext(os.path.basename(fpath))[0])
    idx = 0
    for resource in body:
        if resource.get("metadata", {}).get("name") is None:
            resource["metadata"] = resource.get("metadata", {})
            resource["metadata"]["name"] = test_name if idx == 0 else f"{test_name}-{str(idx)}"
            idx += 1

    runner = JobRunner(
        body, auto_load_kube_config=True, logger=logging, delete_policy=JobRunnerDeletePolicy.Always
    )  # type:ignore
    runner.execute_job()


def test_job():
    _exec_test("../dags/templates/test_job.success.yaml")


def test_pod():
    _exec_test("../dags/templates/test_pod.fail.yaml")


def test_custom():
    _exec_test("../dags/.local/test_custom.fail.yaml")


if __name__ == "__main__":
    test_pod()
    test_job()
    test_custom()
