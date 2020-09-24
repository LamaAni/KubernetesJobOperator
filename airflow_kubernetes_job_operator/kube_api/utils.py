from logging import Logger
from typing import List
from airflow_kubernetes_job_operator.kube_api.exceptions import KubeApiException
import logging

kube_logger: Logger = logging.getLogger(__name__)


def clean_dictionary_nulls(d: dict) -> dict:
    if d is None:
        return {}
    for k in list(d.keys()):
        if d[k] is None:
            del d[k]

    return d


def unqiue_with_order(lst) -> list:
    ulst = []
    uvals = set()
    for v in lst:
        if v in uvals:
            continue
        uvals.add(v)
        ulst.append(v)
    return ulst


def get_apply_uri_from_kind(kind: str, namespace: str):
    if kind == "Job":
        return f"/apis/batch/v1/namespaces/{namespace}/jobs"
    elif kind == "Pod":
        return f"/apis/batch/v1/namespaces/{namespace}/pods"
    elif kind == "Deployment":
        return f"/apis/batch/v1/namespaces/{namespace}/deployments"
    else:
        raise KubeApiException("Unable to resolve kind: " + kind)


def not_empty_string(val: str):
    return isinstance(val, str) and len(val) > 0


def join_locations_list(*args) -> List[str]:
    lcoations = []
    v = None
    for v in args:
        if v is None or (isinstance(v, str) and len(v.strip()) == ""):
            continue
        if isinstance(v, list):
            lcoations += v
        else:
            lcoations += v.split(",")
    return unqiue_with_order(lcoations)
