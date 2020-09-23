from tests.utils import logging
from airflow_kubernetes_job_operator.kube_api import KubeObjectKind
from airflow_kubernetes_job_operator.kube_api import KubeApiRestClient
from airflow_kubernetes_job_operator.kube_api import NamespaceWatchQuery

KubeObjectKind.register_global_kind(
    KubeObjectKind("HCjob", "hc.dto.cbsinteractive.com/v1alpha1", parse_kind_state=KubeObjectKind.parse_state_job)
)


client = KubeApiRestClient()
watcher = NamespaceWatchQuery(watch_pod_logs=True)
watcher.pipe_to_logger()

rslt = client.query_async(watcher)

logging.info(f"Waiting for watch @ {client.get_default_namespace()}...")
watcher.wait_until_running()
logging.info(f"Starting watch @ {client.get_default_namespace()}...")
watcher.join()
