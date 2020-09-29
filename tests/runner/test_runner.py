import os
import yaml
from utils import logging, load_default_kube_config
from airflow_kubernetes_job_operator.job_runner import JobRunner
from airflow_kubernetes_job_operator.kube_api import KubeObjectKind, KubeApiConfiguration

load_default_kube_config()

KubeApiConfiguration.register_kind(
    "HCJob", "hc.dto.cbsinteractive.com/v1alpha1", parse_kind_state=KubeObjectKind.parse_state_job
)


def load_yaml_obj_configs(fpath: str) -> dict:
    if fpath.startswith("./") or fpath.startswith("../"):
        fpath = os.path.join(os.path.dirname(__file__), fpath)
    fpath = os.path.abspath(fpath)
    assert os.path.isfile(fpath), Exception(f"{fpath} is not a file or dose not exist")
    col = []
    with open(fpath, "r") as raw:
        col = list(yaml.safe_load_all(raw.read()))
    return col  # type:ignore


def _exec_test(body: dict):
    runner = JobRunner(body, auto_load_kube_config=True, logger=logging)  # type:ignore
    runner.execute_job()


def test_job():
    _exec_test(load_yaml_obj_configs("./test_job.yaml"))


def test_pod():
    _exec_test(load_yaml_obj_configs("./test_pod.yaml"))


def test_custom():
    _exec_test(load_yaml_obj_configs("../../.local/test_custom.yaml"))


if __name__ == "__main__":
    test_job()
    test_pod()
