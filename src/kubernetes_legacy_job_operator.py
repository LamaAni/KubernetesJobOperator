from .kubernetes_job_operator import KubernetesJobOperator
from typing import List
import os
from airflow.contrib.kubernetes.kubernetes_request_factory import pod_request_factory
from airflow.contrib.kubernetes import kube_client, pod_generator, pod_launcher
from airflow.contrib.kubernetes.pod import Port
from airflow.contrib.kubernetes.volume_mount import VolumeMount
from airflow.contrib.kubernetes.volume import Volume
from airflow.contrib.kubernetes.secret import Secret

# FIXME: remove
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator

JOB_YAML_DEFAULT_FILE = os.path.abspath(f"{__file__}.job.yaml")


class KubernetesLegacyJobOperator(KubernetesJobOperator):
    def __init__(
        self,
        namespace: str = None,
        image: str = None,
        name: str = None,
        cmds: List[str] = None,
        arguments: List[str] = None,
        ports: List[Port] = None,
        volume_mounts: List[VolumeMount] = None,
        volumes: List[Volume] = None,
        env_vars: dict = None,
        secrets: List[Secret] = None,
        in_cluster: bool = False,
        cluster_context: str = None,
        labels: dict = None,
        startup_timeout_seconds: float = 120,
        get_logs: bool = True,
        image_pull_policy: str = "IfNotPresent",
        annotations: dict = None,
        resources=None,  # no type since it depends on version.
        affinity: dict = None,
        config_file: str = None,
        node_selectors: dict = None,
        image_pull_secrets: str = None,
        service_account_name: str = "default",
        is_delete_operator_pod: bool = False,
        hostnetwork: bool = False,
        tolerations: List[dict] = None,
        configmaps: list[str] = None,
        security_context: dict = None,
        pod_runtime_info_envs: dict = None,
        dnspolicy: str = None,
        ## new args.
        job_yaml: str = None,
        job_yaml_filepath: str = None,
        delete_policy: str = None,
        validate_yaml_on_init: bool = configuration.conf.getboolean(
            "kube_job_operator", "VALIDATE_YAML_ON_INIT", fallback=False
        )
        or False,
        *args,
        **kwargs,
    ):
        delete_policy = delete_policy or "IfSucceeded" if is_delete_operator_pod else "Never"

        super().__init__(
            command=cmds,
            arguments=arguments,
            image=image,
            namespace=namespace,
            name=name,
            job_yaml=job_yaml,
            job_yaml_filepath=job_yaml_filepath,
            delete_policy=delete_policy,
            in_cluster=in_cluster,
            config_file=config_file,
            cluster_context=cluster_context,
            validate_yaml_on_init=validate_yaml_on_init,
            startup_timeout_seconds=startup_timeout_seconds,
            *args,
            **kwargs,
        )

        # adding self properties.
        self.image = image
        self.namespace = namespace
        self.cmds = cmds or []
        self.arguments = arguments or []
        self.labels = labels or {}
        self.startup_timeout_seconds = startup_timeout_seconds
        self.name = name
        self.env_vars = env_vars or {}
        self.ports = ports or []
        self.volume_mounts = volume_mounts or []
        self.volumes = volumes or []
        self.secrets = secrets or []
        self.get_logs = get_logs
        self.image_pull_policy = image_pull_policy
        self.node_selectors = node_selectors or {}
        self.annotations = annotations or {}
        self.affinity = affinity or {}
        self.resources = self._set_resources(resources)
        self.image_pull_secrets = image_pull_secrets
        self.service_account_name = service_account_name
        self.hostnetwork = hostnetwork
        self.tolerations = tolerations or []
        self.configmaps = configmaps or []
        self.security_context = security_context or {}
        self.pod_runtime_info_envs = pod_runtime_info_envs or []
        self.dnspolicy = dnspolicy

    def pre_execute(self, context):
        # creating the job yaml.
        super().pre_execute(context)

        gen = pod_generator.PodGenerator()

        for port in self.ports:
            gen.add_port(port)
        for mount in self.volume_mounts:
            gen.add_mount(mount)
        for volume in self.volumes:
            gen.add_volume(volume)

        pod = gen.make_pod(
            namespace=self.namespace,
            image=self.image,
            pod_id=self.name,
            cmds=self.cmds,
            arguments=self.arguments,
            labels=self.labels,
        )

        pod.service_account_name = self.service_account_name
        pod.secrets = self.secrets
        pod.envs = self.env_vars
        pod.image_pull_policy = self.image_pull_policy
        pod.image_pull_secrets = self.image_pull_secrets
        pod.annotations = self.annotations
        pod.resources = self.resources
        pod.affinity = self.affinity
        pod.node_selectors = self.node_selectors
        pod.hostnetwork = self.hostnetwork
        pod.tolerations = self.tolerations
        pod.configmaps = self.configmaps
        pod.security_context = self.security_context
        pod.pod_runtime_info_envs = self.pod_runtime_info_envs
        pod.dnspolicy = self.dnspolicy

        pod_yaml = pod_request_factory.SimplePodRequestFactory().create(pod)

        # setting the pod template.
        self.job_yaml["spec"]["template"]["spec"] = pod_yaml["spec"]

