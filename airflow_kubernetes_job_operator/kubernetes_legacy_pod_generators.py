from typing import TYPE_CHECKING, List
from airflow_kubernetes_job_operator.exceptions import KubernetesJobOperatorException
from airflow_kubernetes_job_operator.config import AIRFLOW_MAJOR_VERSION


# Loading libraries with backwards compatability
if AIRFLOW_MAJOR_VERSION < 2:

    def create_legacy_kubernetes_pod_airflow_from_provider(*args, **kwargs):
        raise KubernetesJobOperatorException("Kubernetes legacy job operator is supported for airflow 2.0 and up")

else:

    from kubernetes.client import models as k8s
    from airflow.kubernetes.secret import Secret  # noqa F401

    if TYPE_CHECKING:
        from airflow_kubernetes_job_operator.kubernetes_legacy_job_operator import KubernetesLegacyJobOperator

    def create_legacy_kubernetes_pod_airflow_from_provider(operator: "KubernetesLegacyJobOperator"):
        env_vars = []
        if isinstance(env_vars, dict):
            for k in env_vars.keys():
                val = env_vars[k]
                val_from = None
                if isinstance(val, dict):
                    val_from = val
                    val = None
                env_vars.append(k8s.V1EnvVar(name=k, value=val, value_from=val_from))

        elif isinstance(operator.env_vars, list):
            for env in operator.env_vars:
                if isinstance(env, k8s.V1EnvVar):
                    env_vars.append(env)

        if operator.pod_runtime_info_envs:
            env_vars.extend([env.to_k8s_client_obj() for env in operator.pod_runtime_info_envs])

        env_from: List[k8s.V1EnvFromSource] = operator.env_from or []

        for map in operator.configmaps:
            if isinstance(map, str):
                env_from.append(k8s.V1EnvFromSource(config_map_ref=map))
            else:
                env_from.append(map)

        pod = operator.full_pod_spec or k8s.V1Pod(
            api_version="v1",
            kind="Pod",
            metadata=k8s.V1ObjectMeta(
                namespace=operator.namespace,
                labels=operator.labels,
                name=operator.name_prefix,
                annotations=operator.annotations,
            ),
            spec=k8s.V1PodSpec(
                node_selector=operator.node_selector,
                affinity=operator.affinity,
                tolerations=operator.tolerations,
                init_containers=operator.init_containers,
                containers=[
                    k8s.V1Container(
                        image=operator.image,
                        name=operator.BASE_CONTAINER_NAME,
                        command=operator.command,
                        ports=operator.ports,
                        image_pull_policy=operator.image_pull_policy,
                        resources=operator.container_resources,
                        volume_mounts=operator.volume_mounts,
                        args=operator.arguments,
                        env=operator.env_vars,
                        env_from=operator.env_from,
                        security_context=operator.container_security_context,
                    )
                ],
                image_pull_secrets=operator.image_pull_secrets,
                service_account_name=operator.service_account_name,
                host_network=operator.hostnetwork,
                security_context=operator.security_context,
                dns_policy=operator.dnspolicy,
                scheduler_name=operator.schedulername,
                restart_policy="Never",
                priority_class_name=operator.priority_class_name,
                volumes=operator.volumes,
            ),
        )

        for secret in operator.secrets:
            pod = secret.attach_to_pod(pod)

        return {
            "metadata": operator.job_runner.client.api_client.sanitize_for_serialization(pod.metadata),
            "spec": operator.job_runner.client.api_client.sanitize_for_serialization(pod.spec),
        }


def create_legacy_kubernetes_pod(operator: "KubernetesLegacyJobOperator"):
    return create_legacy_kubernetes_pod_airflow_from_provider(operator)
