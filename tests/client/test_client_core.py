from tests.utils import logging
from airflow_kubernetes_job_operator.kube_api.client import KubeApiRestClient, KubeApiRestQuery
from airflow_kubernetes_job_operator.kube_api.queries import GetPodLogs

client = KubeApiRestClient()

rslt = client.stream(GetPodLogs("tester", follow=True))

for v in rslt:
    logging.info(v.__repr__())

