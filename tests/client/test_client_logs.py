from tests.utils import logging
from airflow_kubernetes_job_operator.kube_api import KubeResourceKind
from airflow_kubernetes_job_operator.kube_api import KubeApiRestClient
from airflow_kubernetes_job_operator.kube_api import GetPodLogs
from airflow_kubernetes_job_operator.kube_api import kube_logger

KubeResourceKind.register_global_kind(
    KubeResourceKind("HCjob", "hc.dto.cbsinteractive.com/v1alpha1", parse_kind_state=KubeResourceKind.parse_state_job)
)

kube_logger.level = logging.DEBUG

client = KubeApiRestClient()
logger = GetPodLogs(
    name="logs-tester",
    namespace=client.get_default_namespace(),
    follow=True,
)
logger.pipe_to_logger()

rslt = client.query_async(logger)

label = f"{logger.namespace}/{logger.name}"

logging.info(f"Waiting for logs @ {label}...")
logger.wait_until_running()
logging.info(f"Starting logs watch @ {label}...")
logger.join()
logging.info(f"Watch for logs ended @ {label}")
