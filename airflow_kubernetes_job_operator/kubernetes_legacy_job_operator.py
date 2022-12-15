import warnings
import kubernetes.client as k8s

from typing import List, Optional, Union, Dict
from airflow_kubernetes_job_operator.exceptions import KubernetesJobOperatorException
from airflow_kubernetes_job_operator.kube_api.collections import KubeResourceDescriptor
from airflow_kubernetes_job_operator.job_runner import JobRunnerDeletePolicy
from airflow_kubernetes_job_operator.utils import resolve_relative_path, dict_merge, dict_remove_empty_cols
from airflow_kubernetes_job_operator.kubernetes_job_operator import KubernetesJobOperator, xcom_value_parser
from airflow_kubernetes_job_operator.config import (
    DEFAULT_VALIDATE_BODY_ON_INIT,
    DEFAULT_EXECUTION_OBJECT_PATHS,
    KubernetesJobOperatorDefaultExecutionResource,
)

from airflow_kubernetes_job_operator.kubernetes_legacy_pod_generators import (
    create_legacy_kubernetes_pod,
    Volume,
    Port,
    VolumeMount,
    Secret,
    Resources,
    Affinity,
    PodRuntimeInfoEnv,
)


class KubernetesLegacyJobOperator(KubernetesJobOperator):
    def __init__(
        self,
        *args,
        namespace: Optional[str] = None,
        image: Optional[str] = None,
        name: Optional[str] = None,
        cmds: Optional[List[str]] = None,
        arguments: Optional[List[str]] = None,
        ports: Optional[List[Port]] = None,
        volume_mounts: Optional[List[VolumeMount]] = None,
        volumes: Optional[List[Volume]] = None,
        env_vars: Optional[Union[dict, List[k8s.V1EnvVar]]] = None,
        env_from: Optional[List[k8s.V1EnvFromSource]] = None,
        secrets: Optional[List[Secret]] = None,
        in_cluster: Optional[bool] = None,
        cluster_context: Optional[str] = None,
        labels: Optional[Dict] = None,
        reattach_on_restart: bool = True,
        startup_timeout_seconds: int = 120,
        get_logs: bool = True,
        image_pull_policy: Optional[str] = None,
        annotations: Optional[Dict] = None,
        resources: Optional[Resources] = None,
        affinity: Optional[Affinity] = None,
        config_file: Optional[str] = None,
        node_selectors: Optional[dict] = None,
        node_selector: Optional[dict] = None,
        image_pull_secrets: Optional[Union[List[k8s.V1LocalObjectReference], str]] = None,
        service_account_name: Optional[str] = None,
        is_delete_operator_pod: bool = False,
        hostnetwork: bool = False,
        tolerations: Optional[List[k8s.V1Toleration]] = None,
        security_context: Optional[Dict] = None,
        dnspolicy: Optional[str] = None,
        schedulername: Optional[str] = None,
        full_pod_spec: Optional[k8s.V1Pod] = None,
        init_containers: Optional[List[k8s.V1Container]] = None,
        log_events_on_failure: bool = False,
        do_xcom_push: bool = False,
        pod_template_file: Optional[str] = None,
        priority_class_name: Optional[str] = None,
        pod_runtime_info_envs: List[PodRuntimeInfoEnv] = None,
        termination_grace_period: Optional[int] = None,
        configmaps: Optional[str] = None,
        # job operator args
        body: str = None,
        body_filepath: str = None,
        delete_policy: Union[str, JobRunnerDeletePolicy] = None,
        validate_body_on_init: bool = DEFAULT_VALIDATE_BODY_ON_INIT,
        enable_jinja: bool = True,
        jinja_job_args: dict = None,
        on_kube_api_event: callable = None,
        parse_xcom_event: xcom_value_parser = xcom_value_parser,
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
        :param name: The name prefix for the executing pod. (default: task_id)
        :type name: str
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
            on_kube_api_event {callable, optional} -- a method to catch api events when called. lambda api_event,
                context: ...
            parse_xcom_event {xcom_value_parser, optional} -- parse an incoming xcom event value.
                Must return a dictionary with key/value pairs.
        """
        delete_policy = (
            delete_policy or JobRunnerDeletePolicy.IfSucceeded
            if is_delete_operator_pod
            else JobRunnerDeletePolicy.Never
        )

        body_filepath = body_filepath or pod_template_file

        if body_filepath is not None:
            body_filepath = resolve_relative_path(body_filepath, 2)

        if full_pod_spec is not None and body_filepath is None:
            body_filepath = DEFAULT_EXECUTION_OBJECT_PATHS[KubernetesJobOperatorDefaultExecutionResource.Pod]

        super().__init__(
            command=cmds or [],
            arguments=arguments or [],
            image=image,
            name_prefix=name,
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
            on_kube_api_event=on_kube_api_event,
            parse_xcom_event=parse_xcom_event,
            *args,
            **kwargs,
        )

        assert image is not None, "You must provide an image"

        # If true, loaded from yaml and should not override spec.
        self.loaded_from_full_spec = full_pod_spec is not None or body is not None or body_filepath is not None

        # adding self properties.
        self.labels = labels or {}
        self.ports = ports or []
        self.volume_mounts = volume_mounts or []
        self.volumes = volumes or []
        self.secrets = secrets or []
        if node_selectors:
            # Node selectors is incorrect based on k8s API
            warnings.warn("node_selectors is deprecated. Please use node_selector instead.", DeprecationWarning)
            self.node_selector = node_selectors
        elif node_selector:
            self.node_selector = node_selector
        else:
            self.node_selector = {}

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

        # new values.
        self.full_pod_spec = full_pod_spec
        self.termination_grace_period = termination_grace_period

        # Value is ignored. Xcoms are always active on KJO (using logging)
        self._ignored_log_events_on_failure = log_events_on_failure
        self._ignored_do_xcom_push = do_xcom_push
        self._unsupported_reattach_on_restart = reattach_on_restart

        self.init_containers = init_containers
        self.env_from = env_from
        self.env_vars = env_vars
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
        pod = KubeResourceDescriptor(
            create_legacy_kubernetes_pod(self),
        )

        if "name" in pod.metadata:
            self.name_prefix = self.name_prefix or pod.metadata["name"]
            del pod.metadata["name"]

        dict_remove_empty_cols(pod.body)

        pod.body["metadata"] = pod.body.get("metadata", {})
        pod.body["spec"] = pod.body.get("spec", {})

        super().prepare_and_update_body()

        if len(self.job_runner.body) == 0:
            self.job_runner.body[0] = {
                "apiVersion": "batch/v1",
                "kind": "Pod",
                "metadata": {},
                "spec": {},
            }

        run_template = KubeResourceDescriptor(self.job_runner.body[0])
        if run_template.kind_name == "pod":
            dict_merge(self.job_runner.body[0], pod.body)
        elif run_template.kind_name == "job":
            self.job_runner.body[0]["spec"]["template"] = {
                "metadata": pod.body.get("metadata"),
                "spec": pod.body.get("spec"),
            }
        else:
            raise KubernetesJobOperatorException(
                "Invalid kind in operator template. Expected first element run kind to either be Job or Pod"
            )
