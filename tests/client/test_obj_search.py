from tests.utils import logging
from airflow_kubernetes_job_operator.kube_api import Event

from airflow_kubernetes_job_operator.kube_api import KubeObjectKind
from airflow_kubernetes_job_operator.kube_api import KubeApiRestClient
from airflow_kubernetes_job_operator.kube_api import GetNamespaceObjects

KubeObjectKind.register_global_kind(
    KubeObjectKind("HCjob", "hc.dto.cbsinteractive.com/v1alpha1", parse_kind_state=KubeObjectKind.parse_state_job)
)

client = KubeApiRestClient()
query = GetNamespaceObjects("Pod", namespace=client.get_default_namespace())

logging.info(f"Looking for {query.resource_path}...")
ev: Event = None
for ev in client.stream(query):
    if ev.name == query.data_event_name:
        logging.info(ev.args[0]["metadata"]["name"])
