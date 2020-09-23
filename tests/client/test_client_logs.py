from tests.utils import logging
from airflow_kubernetes_job_operator.kube_api import KubeObjectKind
from airflow_kubernetes_job_operator.kube_api import KubeApiRestClient
from airflow_kubernetes_job_operator.kube_api import GetPodLogs

KubeObjectKind.register_global_kind(
    KubeObjectKind("HCjob", "hc.dto.cbsinteractive.com/v1alpha1", parse_kind_state=KubeObjectKind.parse_state_job)
)


client = KubeApiRestClient()
logger = GetPodLogs(
    name="hc-job-controller-77c55c54b-bv9r8",
    namespace=client.get_default_namespace(),
    follow=True,
)
logger.pipe_to_logger()

rslt = client.query_async(logger)

label = f"{logger.namespace}/{logger.name}"

logging.info(f"Waiting for logs @ {label}...")
logger.wait_until_running()
logging.info(f"Starting watch @ {label}...")
logger.join()
logging.info(f"Starting watch @ {label}...")
