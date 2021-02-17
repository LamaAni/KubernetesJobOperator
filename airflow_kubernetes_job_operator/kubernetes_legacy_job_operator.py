import kubernetes.client as k8s

from typing import List, Optional, Union
from airflow_kubernetes_job_operator.job_runner import JobRunnerDeletePolicy
from airflow_kubernetes_job_operator.utils import resolve_relative_path
from airflow_kubernetes_job_operator.kubernetes_job_operator import KubernetesJobOperator
from airflow_kubernetes_job_operator.config import DEFAULT_VALIDATE_BODY_ON_INIT

try:
    from airflow.contrib.kubernetes.kubernetes_request_factory import pod_request_factory
    from airflow.contrib.kubernetes import pod_generator
    from airflow.contrib.kubernetes.pod import Resources
    from airflow.contrib.kubernetes.volume_mount import VolumeMount
    from airflow.contrib.kubernetes.volume import Volume
    from airflow.contrib.kubernetes.secret import Secret
except Exception:
    from kubernetes.client import (
        V1ResourceRequirements as Resources,
        V1Volume as Volume,
        V1VolumeMount as VolumeMount,
        V1Secret as Secret,
    )

    pod_generator = None
    pod_request_factory = None


class KubernetesLegacyJobOperator(KubernetesJobOperator):
    def __init__(
        self,
        namespace: str = None,
        image: str = None,
        cmds: List[str] = None,
        arguments: List[str] = None,
        ports: list = None,
        volume_mounts: List[VolumeMount] = None,
        volumes: List[Volume] = None,
        env_vars: dict = None,
        secrets: List[Secret] = None,
        in_cluster: bool = None,
        cluster_context: str = None,
        labels: dict = None,
        startup_timeout_seconds: float = 120,
        get_logs: bool = True,
        image_pull_policy: str = "IfNotPresent",
        annotations: dict = None,
        resources=None,
        affinity: dict = None,
        config_file: str = None,
        node_selectors: dict = None,
        image_pull_secrets: str = None,
        service_account_name: str = "default",
        is_delete_operator_pod: bool = False,
        hostnetwork: bool = False,
        tolerations: List[dict] = None,
        configmaps: List[str] = None,
        security_context: dict = None,
        pod_runtime_info_envs: dict = None,
        dnspolicy: str = None,
        # new args.
        init_containers: Optional[List[k8s.V1Container]] = None,
        env_from: List[str] = None,
        schedulername: str = None,
        priority_class_name: str = None,
        # job operator args
        body: str = None,
        body_filepath: str = None,
        delete_policy: Union[str, JobRunnerDeletePolicy] = None,
        validate_body_on_init: bool = DEFAULT_VALIDATE_BODY_ON_INIT,
        enable_jinja: bool = True,
        jinja_job_args: dict = None,
        *args,
        **kwargs,
    ):
        """
        A operator that executes an airflow task as a kubernetes Job.
        See: https://kubernetes.io/docs/concepts/workloads/controllers/jobs-run-to-completion/
            for notes about kubernetes jobs.

        NOTE: This is a legacy operator that allows for similar arguments
        as the KubernetesPodOperator. Please use the KubernetesJobOperator instead.

        NOTE: xcom has not been implemented.

        :param image: Docker image you wish to launch. Defaults to dockerhub.io,
            but fully qualified URLS will point to custom repositories
        :type image: str
        :param namespace: the namespace to run within kubernetes
        :type namespace: str
        :param cmds: entrypoint of the container. (templated)
            The docker images's entrypoint is used if this is not provide.
        :type cmds: list[str]
        :param arguments: arguments of the entrypoint. (templated)
            The docker image's CMD is used if this is not provided.
        :type arguments: list[str]
        :param image_pull_policy: Specify a policy to cache or always pull an image
        :type image_pull_policy: str
        :param image_pull_secrets: Any image pull secrets to be given to the pod.
                                If more than one secret is required, provide a
                                comma separated list: secret_a,secret_b
        :type image_pull_secrets: str
        :param ports: ports for launched pod
        :type ports: list
        :param volume_mounts: volumeMounts for launched pod
        :type volume_mounts: list[airflow.contrib.kubernetes.volume_mount.VolumeMount]
        :param volumes: volumes for launched pod. Includes ConfigMaps and PersistentVolumes
        :type volumes: list[airflow.contrib.kubernetes.volume.Volume]
        :param labels: labels to apply to the Pod
        :type labels: dict
        :param startup_timeout_seconds: timeout in seconds to startup the pod
        :type startup_timeout_seconds: int
        :param name: name of the task you want to run,
            will be used to generate a pod id
        :type name: str
        :param env_vars: Environment variables initialized in the container. (templated)
        :type env_vars: dict
        :param secrets: Kubernetes secrets to inject in the container,
            They can be exposed as environment vars or files in a volume.
        :type secrets: list[airflow.contrib.kubernetes.secret.Secret]
        :param in_cluster: run kubernetes client with in_cluster configuration (if None autodetect)
        :type in_cluster: bool
        :param cluster_context: context that points to kubernetes cluster.
            Ignored when in_cluster is True. If None, current-context is used.
        :type cluster_context: str
        :param get_logs: get the stdout of the container as logs of the tasks
        :type get_logs: bool
        :param annotations: non-identifying metadata you can attach to the Pod.
                            Can be a large range of data, and can include characters
                            that are not permitted by labels.
        :type annotations: dict
        :param resources: A dict containing a group of resources requests and limits
        :type resources: dict
        :param affinity: A dict containing a group of affinity scheduling rules
        :type affinity: dict
        :param node_selectors: A dict containing a group of scheduling rules
        :type node_selectors: dict
        :param config_file: The path to the Kubernetes config file
        :type config_file: str
        :param is_delete_operator_pod: What to do when the pod reaches its final
            state, or the execution is interrupted.
            If False (default): do nothing, If True: delete the pod if succeeded
        :type is_delete_operator_pod: bool
        :param hostnetwork: If True enable host networking on the pod
        :type hostnetwork: bool
        :param tolerations: A list of kubernetes tolerations
        :type tolerations: list tolerations
        :param configmaps: A list of configmap names objects that we
            want mount as env variables
        :type configmaps: list[str]
        :param pod_runtime_info_envs: environment variables about
                                    pod runtime information (ip, namespace, nodeName, podName)
        :type pod_runtime_info_envs: list[PodRuntimeEnv]
        :param dnspolicy: Specify a dnspolicy for the pod
        :type dnspolicy: str

        Added arguments:

            body {dict|string} -- The job to execute as a yaml description. (default: None)
            body_filepath {str} -- The path to the file to read the yaml from, overridden by
                body. (default: None)
            delete_policy {str} -- Any of: Never, Always, IfSucceeded (default: {"IfSucceeded"});
                overrides is_delete_operator_pod.
            validate_body_on_init {bool} -- If true, validates the yaml in the constructor,
                setting this to True, will slow dag creation.
                (default: {from env/airflow config: AIRFLOW__KUBE_JOB_OPERATOR__validate_body_on_init or False})
            jinja_job_args {dict} -- A dictionary or object to be used in the jinja template to render
                arguments. The jinja args are loaded under the keyword "job".
        """
        delete_policy = (
            delete_policy or JobRunnerDeletePolicy.IfSucceeded
            if is_delete_operator_pod
            else JobRunnerDeletePolicy.Never
        )

        if body_filepath is not None:
            body_filepath = resolve_relative_path(body_filepath, 2)

        super().__init__(
            command=cmds or [],
            arguments=arguments or [],
            envs=env_vars or {},
            image=image,
            namespace=namespace,
            body=body,
            body_filepath=body_filepath,
            delete_policy=delete_policy,
            in_cluster=in_cluster,
            config_file=config_file,
            cluster_context=cluster_context,
            validate_body_on_init=validate_body_on_init,
            startup_timeout_seconds=startup_timeout_seconds,
            get_logs=get_logs,
            enable_jinja=enable_jinja,
            image_pull_policy=image_pull_policy,
            jinja_job_args=jinja_job_args,
            *args,
            **kwargs,
        )

        # adding self properties.
        self.labels = labels or {}
        self.ports = ports or []
        self.volume_mounts = volume_mounts or []
        self.volumes = volumes or []
        self.secrets = secrets or []
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

        self.init_containers = init_containers
        self.env_from = env_from
        self.schedulername = schedulername
        self.priority_class_name = priority_class_name

    def _set_resources(self, resources):
        # Legacy
        inputResource = Resources()
        if resources:
            for item in resources.keys():
                setattr(inputResource, item, resources[item])
        return inputResource

    def prepare_and_update_body(self):
        # call to prepare the raw body.
        super().prepare_and_update_body()

        pod_body = None
        if pod_generator is not None:
            # old pod generator
            gen = pod_generator.PodGenerator()

            for port in self.ports:
                gen.add_port(port)
            for mount in self.volume_mounts:
                gen.add_mount(mount)
            for volume in self.volumes:
                gen.add_volume(volume)

            job_obj = self.job_runner.body[0]

            # selecting appropriate pod values.
            all_labels = {}
            all_labels.update(self.labels)
            all_labels.update(job_obj["spec"]["template"]["metadata"].get("labels", {}))
            image = self.image or job_obj["spec"]["template"]["spec"]["containers"][0].get("image", None)
            cmds = self.command or job_obj["spec"]["template"]["spec"]["containers"][0].get("command", [])
            arguments = self.arguments or job_obj["spec"]["template"]["spec"]["containers"][0].get("args", [])

            pod = gen.make_pod(
                namespace=job_obj["metadata"]["namespace"],
                image=image,
                pod_id=job_obj["metadata"]["name"],
                cmds=cmds,
                arguments=arguments,
                labels=all_labels,
            )

            pod.service_account_name = self.service_account_name
            pod.secrets = self.secrets
            pod.envs = self.envs
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

            # old pod generation.. moving to new one
            pod_body = pod_request_factory.SimplePodRequestFactory().create(pod)
        else:
            pod_body = k8s.V1Pod(
                api_version="v1",
                kind="Pod",
                metadata=k8s.V1ObjectMeta(
                    namespace=self.namespace,
                    labels=self.labels,
                    name="legacy",
                    annotations=self.annotations,
                ),
                spec=k8s.V1PodSpec(
                    node_selector=self.node_selectors,
                    affinity=self.affinity,
                    tolerations=self.tolerations,
                    init_containers=self.init_containers,
                    containers=[
                        k8s.V1Container(
                            image=self.image,
                            name="main",
                            command=self.command,
                            ports=self.ports,
                            resources=self.resources,
                            volume_mounts=self.volume_mounts,
                            args=self.arguments,
                            env=None if self.envs is None else self._get_kubernetes_env_list(),
                            env_from=self.env_from,
                        )
                    ],
                    image_pull_secrets=self.image_pull_secrets,
                    service_account_name=self.service_account_name,
                    host_network=self.hostnetwork,
                    security_context=self.security_context,
                    dns_policy=self.dnspolicy,
                    scheduler_name=self.schedulername,
                    restart_policy="Never",
                    priority_class_name=self.priority_class_name,
                    volumes=self.volumes,
                ),
            )
            pod_body = {
                "metadata": self.job_runner.client.api_client.sanitize_for_serialization(pod_body.metadata),
                "spec": self.job_runner.client.api_client.sanitize_for_serialization(pod_body.spec),
            }

        # reset the name
        del pod_body["metadata"]["name"]
        pod_body["metadata"].update(self.job_runner.body[0]["spec"]["template"]["metadata"])
        self.job_runner.body[0]["spec"]["template"] = pod_body
