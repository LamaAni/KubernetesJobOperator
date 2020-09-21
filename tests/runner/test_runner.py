import os
import yaml
from tests.utils import logging
from airflow_kubernetes_job_operator.job_runner import JobRunner


def load_yaml_obj_configs(fpath: str):
    if fpath.startswith("./") or fpath.startswith("../"):
        fpath = os.path.join(os.path.dirname(__file__), fpath)
    fpath = os.path.abspath(fpath)
    assert os.path.isfile(fpath), Exception(f"{fpath} is not a file or dose not exist")
    col = []
    with open(fpath, "r") as raw:
        col = list(yaml.safe_load_all(raw.read()))
    return col


def test_runner():
    body = load_yaml_obj_configs("./test_job.yaml")
    runner = JobRunner(body, auto_load_kube_config=True, delete_on_failure=True, logger=logging)  # type:ignore
    runner.execute_job()


if __name__ == "__main__":
    test_runner()
