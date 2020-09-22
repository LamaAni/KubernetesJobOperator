import os
from kubernetes.config import kube_config, incluster_config
from airflow_kubernetes_job_operator.kube_api.utils import join_locations_list


DEFAULT_KUBE_CONFIG_LOCATIONS = join_locations_list(
    [kube_config.KUBE_CONFIG_DEFAULT_LOCATION],
    os.environ.get("KUBERNETES_JOB_OPERATOR_DEFAULT_CONFIG_LOCATIONS", None),
)
DEFAULT_SERVICE_ACCOUNT_PATH = os.path.dirname(incluster_config.SERVICE_CERT_FILENAME)
DEFAULT_USE_ASYNCIO_ENV_NAME = "KUBERNETES_API_CLIENT_USE_ASYNCIO"

KURBETNTES_API_ACCEPT = [
    "application/json",
    "application/yaml",
    "application/vnd.kubernetes.protobuf",
]

KUBERENTES_API_CONTENT_TYPES = [
    "application/json",
    "application/json-patch+json",
    "application/merge-patch+json",
    "application/strategic-merge-patch+json",
]

DEFAULT_AUTO_RECONNECT_MAX_ATTEMPTS = 60
DEFAULT_AUTO_RECONNECT_WAIT_BETWEEN_ATTEMPTS = 5