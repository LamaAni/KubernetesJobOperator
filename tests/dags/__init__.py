from airflow_kubernetes_job_operator.kube_api import KubeObjectKind, KubeApiConfiguration
from utils import print_default_kube_configuration

KubeApiConfiguration.add_kube_config_search_location("~/composer_kube_config")  # second
KubeApiConfiguration.add_kube_config_search_location("~/gcs/dags/config/hcjobs-kubeconfig.yaml")  # first
KubeApiConfiguration.set_default_namespace("cdm-hcjobs")

KubeApiConfiguration.register_kind(
    name="HCJob",
    api_version="hc.dto.cbsinteractive.com/v1alpha1",
    parse_kind_state=KubeObjectKind.parse_state_job,
)

print_default_kube_configuration()
