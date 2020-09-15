from tests.utils import logging
from airflow_kubernetes_job_operator.kube_api.client import KubeApiRestClient, KubeApiRestQuery
from airflow_kubernetes_job_operator.kube_api.queries import GetNamespaceObjects
from airflow_kubernetes_job_operator.kube_api.watchers import NamespaceWatchQuery

client = KubeApiRestClient()
rslt = client.stream(NamespaceWatchQuery())

for v in rslt:
    logging.info(v["metadata"].get("selfLink"))
