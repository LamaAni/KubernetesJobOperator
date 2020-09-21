from tests.utils import logging
from airflow_kubernetes_job_operator.kube_api import KubeObjectKind
from airflow_kubernetes_job_operator.kube_api import KubeApiRestClient
from airflow_kubernetes_job_operator.kube_api import NamespaceWatchQuery

KubeObjectKind.register_global_kind(
    KubeObjectKind("HCjob", "hc.dto.cbsinteractive.com/v1alpha1", parse_kind_state=KubeObjectKind.parse_state_job)
)


client = KubeApiRestClient()
query = NamespaceWatchQuery(watch_pod_logs=False)
query.pipe_to_logger()

rslt = client.async_query(query)

logging.info(f"Waiting for watch @ {client.get_default_namespace()}...")
query.wait_until_running()
logging.info(f"Starting watch @ {client.get_default_namespace()}...")
query.join()
