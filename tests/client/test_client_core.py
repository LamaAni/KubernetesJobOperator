from tests.utils import logging
from airflow_kubernetes_job_operator.kube_api.client import KubeApiRestClient
from airflow_kubernetes_job_operator.kube_api.queries import GetPodLogs

client = KubeApiRestClient()

query = GetPodLogs("tester", follow=True)
query.pipe_to_logger()
client.async_query(query)
query.wait_until_running()
logging.info("Starting watch...")
query.join()
