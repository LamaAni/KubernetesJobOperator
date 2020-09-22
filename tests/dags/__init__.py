from airflow_kubernetes_job_operator.kube_api import KubeObjectKind

KubeObjectKind.register_global_kind(
    KubeObjectKind("HCJob", "hc.dto.cbsinteractive.com/v1alpha1", parse_kind_state=KubeObjectKind.parse_state_job)
)
