import kubernetes.client as k8s

from typing import List, Union
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
    Secret,
)


class KubernetesLegacyJobOperator(KubernetesJobOperator):
    BASE_CONTAINER_NAME: str = "main"

    def __init__(
        self,
        *args,
        namespace: str = None,
        image: str = None,
        name: str = None,
        random_name_suffix: bool = True,
        cmds: List[str] = None,
        arguments: List[str] = None,
        ports: List[k8s.V1ContainerPort] = None,
        volume_mounts: List[k8s.V1VolumeMount] = None,
        volumes: List[k8s.V1Volume] = None,
        env_vars: List[k8s.V1EnvVar] = None,
        env_from: List[k8s.V1EnvFromSource] = None,
        secrets: List[Secret] = None,
        in_cluster: bool = None,
        cluster_context: str = None,
        labels: dict = None,
        reattach_on_restart: bool = True,
        startup_timeout_seconds: int = 120,
        get_logs: bool = True,
        image_pull_policy: str = None,
        annotations: dict = None,
        container_resources: k8s.V1ResourceRequirements = None,
        affinity: k8s.V1Affinity = None,
        config_file: str = None,
        node_selector: dict = None,
        image_pull_secrets: List[k8s.V1LocalObjectReference] = None,
        service_account_name: str = None,
        is_delete_operator_pod: bool = True,
        hostnetwork: bool = False,
        tolerations: List[k8s.V1Toleration] = None,
        security_context: dict = None,
        container_security_context: dict = None,
        dnspolicy: str = None,
        schedulername: str = None,
        full_pod_spec: k8s.V1Pod = None,
        init_containers: List[k8s.V1Container] = None,
        log_events_on_failure: bool = False,
        do_xcom_push: bool = False,
        pod_template_file: str = None,
        priority_class_name: str = None,
        pod_runtime_info_envs: List[k8s.V1EnvVar] = None,
        termination_grace_period: int = None,
        configmaps: List[str] = None,
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

        .. seealso::
            For more information on how to use this operator, take a look at the guide:
            :ref:`howto/operator:KubernetesPodOperator`

        .. note::
            If you use `Google Kubernetes Engine <https://cloud.google.com/kubernetes-engine/>`__
            and Airflow is not running in the same cluster, consider using
            :class:`~airflow.providers.google.cloud.operators.kubernetes_engine.GKEStartPodOperator`, which
            simplifies the authorization process.

        :param namespace: the namespace to run within kubernetes.
        :param image: Docker image you wish to launch. Defaults to hub.docker.com,
            but fully qualified URLS will point to custom repositories. (templated)
        :param name: name of the pod in which the task will run, will be used (plus a random
            suffix if random_name_suffix is True) to generate a pod id (DNS-1123 subdomain,
            containing only [a-z0-9.-]).
        :param random_name_suffix: if True, will generate a random suffix.
        :param cmds: entrypoint of the container. (templated)
            The docker images's entrypoint is used if this is not provided.
        :param arguments: arguments of the entrypoint. (templated)
            The docker image's CMD is used if this is not provided.
        :param ports: ports for the launched pod.
        :param volume_mounts: volumeMounts for the launched pod.
        :param volumes: volumes for the launched pod. Includes ConfigMaps and PersistentVolumes.
        :param env_vars: Environment variables initialized in the container. (templated)
        :param env_from: (Optional) List of sources to populate environment variables in the container.
        :param secrets: Kubernetes secrets to inject in the container.
            They can be exposed as environment vars or files in a volume.
        :param in_cluster: run kubernetes client with in_cluster configuration.
        :param cluster_context: context that points to kubernetes cluster.
            Ignored when in_cluster is True. If None, current-context is used.
        :param reattach_on_restart: if the worker dies while the pod is running, reattach and monitor
            during the next try. If False, always create a new pod for each try.
        :param labels: labels to apply to the Pod. (templated)
        :param startup_timeout_seconds: timeout in seconds to startup the pod.
        :param get_logs: get the stdout of the container as logs of the tasks.
        :param image_pull_policy: Specify a policy to cache or always pull an image.
        :param annotations: non-identifying metadata you can attach to the Pod.
            Can be a large range of data, and can include characters
            that are not permitted by labels.
        :param container_resources: resources for the launched pod. (templated)
        :param affinity: affinity scheduling rules for the launched pod.
        :param config_file: The path to the Kubernetes config file. (templated)
            If not specified, default value is ``~/.kube/config``
        :param node_selector: A dict containing a group of scheduling rules.
        :param image_pull_secrets: Any image pull secrets to be given to the pod.
            If more than one secret is required, provide a
            comma separated list: secret_a,secret_b
        :param service_account_name: Name of the service account
        :param is_delete_operator_pod: What to do when the pod reaches its final
            state, or the execution is interrupted. If True (default), delete the
            pod; if False, leave the pod.
        :param hostnetwork: If True enable host networking on the pod.
        :param tolerations: A list of kubernetes tolerations.
        :param security_context: security options the pod should run with (PodSecurityContext).
        :param container_security_context: security options the container should run with.
        :param dnspolicy: dnspolicy for the pod.
        :param schedulername: Specify a schedulername for the pod
        :param full_pod_spec: The complete podSpec
        :param init_containers: init container for the launched Pod
        :param log_events_on_failure: Log the pod's events if a failure occurs
        :param do_xcom_push: If True, the content of the file
            /airflow/xcom/return.json in the container will also be pushed to an
            XCom when the container completes.
        :param pod_template_file: path to pod template file (templated)
        :param priority_class_name: priority class name for the launched Pod
        :param pod_runtime_info_envs: (Optional) A list of environment variables,
            to be set in the container.
        :param termination_grace_period: Termination grace period if task killed in UI,
            defaults to kubernetes default
        :param configmaps: (Optional) A list of names of config maps from which it collects ConfigMaps
            to populate the environment variables with. The contents of the target
            ConfigMap's Data field will represent the key-value pairs as environment variables.
            Extends env_from.

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
        self.node_selector = node_selector or {}
        self.annotations = annotations or {}
        self.affinity = affinity or {}
        self.container_resources = container_resources
        self.image_pull_secrets = image_pull_secrets
        self.service_account_name = service_account_name
        self.hostnetwork = hostnetwork
        self.tolerations = tolerations or []
        self.configmaps = configmaps or []
        self.security_context = security_context or {}
        self.container_security_context = container_security_context or {}
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
