from typing import TYPE_CHECKING, List
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow_kubernetes_job_operator.config import AIRFLOW_MAJOR_VERSION, AIRFLOW_MINOR_VERSION, AIRFLOW_PATCH_VERSION


# Loading libraries with backwards compatability
if AIRFLOW_MAJOR_VERSION == 1 and AIRFLOW_PATCH_VERSION <= 10:
    from airflow.contrib.kubernetes.kubernetes_request_factory import pod_request_factory
    from airflow.contrib.kubernetes import pod_generator
    from airflow.contrib.kubernetes.pod import Resources
    from airflow.contrib.kubernetes.volume_mount import VolumeMount
    from airflow.contrib.kubernetes.volume import Volume
    from airflow.contrib.kubernetes.secret import Secret
    from airflow.contrib.kubernetes.pod import Port
    from typing import Dict as Affinity
    from airflow.contrib.kubernetes.pod_runtime_info_env import PodRuntimeInfoEnv
else:
    if AIRFLOW_MAJOR_VERSION == 1:
        from airflow.contrib.kubernetes.pod_runtime_info_env import PodRuntimeInfoEnv
    elif AIRFLOW_MAJOR_VERSION == 2 and AIRFLOW_MINOR_VERSION < 4:
        from airflow.providers.cncf.kubernetes.backcompat.pod_runtime_info_env import PodRuntimeInfoEnv
    else:
        from airflow.kubernetes.pod_runtime_info_env import PodRuntimeInfoEnv

    from kubernetes.client import CoreV1Api, models as k8s
    from kubernetes.client import (
        V1ResourceRequirements as Resources,
        V1Volume as Volume,
        V1VolumeMount as VolumeMount,
        V1ContainerPort as Port,
        V1Affinity as Affinity,
        V1LocalObjectReference as ImagePullSecret,
    )
    from airflow.kubernetes.secret import Secret
    from kubernetes.client.models import V1Secret


if TYPE_CHECKING:
    from airflow_kubernetes_job_operator.kubernetes_legacy_job_operator import KubernetesLegacyJobOperator


def create_legacy_kubernetes_pod_airflow_1_using_request_factory(operator: "KubernetesLegacyJobOperator"):
    if operator.full_pod_spec:
        raise Exception("full_pod_spec is not allowed in this version of airflow")

    pod_body = None
    # old pod generator
    gen = pod_generator.PodGenerator()

    for port in operator.ports:
        gen.add_port(port)
    for mount in operator.volume_mounts:
        gen.add_mount(mount)
    for volume in operator.volumes:
        gen.add_volume(volume)

    all_labels = {}
    all_labels.update(operator.labels)

    pod = gen.make_pod(
        namespace=operator.namespace,
        image=operator.image,
        pod_id=operator.name_prefix or "kjo",
        cmds=operator.command,
        arguments=operator.arguments,
        labels=all_labels,
    )

    pod.service_account_name = operator.service_account_name
    pod.secrets = operator.secrets
    pod.envs = operator.envs or {}
    pod.image_pull_policy = operator.image_pull_policy
    pod.image_pull_secrets = operator.image_pull_secrets
    pod.annotations = operator.annotations
    pod.resources = operator.resources
    pod.affinity = operator.affinity
    pod.node_selectors = operator.node_selector
    pod.hostnetwork = operator.hostnetwork
    pod.tolerations = operator.tolerations
    pod.configmaps = operator.configmaps
    pod.security_context = operator.security_context
    pod.pod_runtime_info_envs = operator.pod_runtime_info_envs
    pod.dnspolicy = operator.dnspolicy

    # old pod generation.. moving to new one
    pod_body = pod_request_factory.SimplePodRequestFactory().create(pod)

    return pod_body


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
            termination_grace_period_seconds=operator.termination_grace_period,
            node_selector=operator.node_selector,
            affinity=operator.affinity,
            tolerations=operator.tolerations,
            init_containers=operator.init_containers,
            containers=[
                k8s.V1Container(
                    name="main",
                    image=operator.image,
                    command=operator.command,
                    ports=operator.ports,
                    resources=operator.resources,
                    volume_mounts=operator.volume_mounts,
                    args=operator.arguments,
                    env=env_vars,
                    env_from=env_from,
                    image_pull_policy=operator.image_pull_policy,
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
    if AIRFLOW_MAJOR_VERSION == 1 and AIRFLOW_PATCH_VERSION <= 10:
        return create_legacy_kubernetes_pod_airflow_1_using_request_factory(operator)
    return create_legacy_kubernetes_pod_airflow_from_provider(operator)
