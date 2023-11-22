from tests.utils import logging
from airflow_kubernetes_job_operator.kube_api import Event

from airflow_kubernetes_job_operator.kube_api import KubeResourceKind
from airflow_kubernetes_job_operator.kube_api import KubeApiRestClient
from airflow_kubernetes_job_operator.kube_api import GetNamespaceResources

KubeResourceKind.register_global_kind(
    KubeResourceKind(
        "HCjob",
        "hc.dto.cbsinteractive.com/v1alpha1",
        parse_kind_state=KubeResourceKind.parse_state_job,
    )
)

client = KubeApiRestClient()
query = GetNamespaceResources("Pod", namespace=client.get_default_namespace())

logging.info(f"Looking for {query.resource_path}...")
ev: Event = None
for ev in client.stream(query):
    if ev.name == query.data_event_name:
        logging.info(ev.args[0]["metadata"]["name"])
