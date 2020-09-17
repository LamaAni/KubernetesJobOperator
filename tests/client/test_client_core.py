from tests.utils import logging
from airflow_kubernetes_job_operator.kube_api.client import KubeApiRestClient, KubeApiRestQuery, set_asyncio_mode
from airflow_kubernetes_job_operator.kube_api.queries import GetPodLogs

client = KubeApiRestClient()

query = GetPodLogs("tester", follow=True)
query.bind_logger()
client.async_query(query)
query.wait_until_started()
logging.info("Starting watch...")
query.join()
