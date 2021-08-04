import jinja2
import json
from typing import List, Union
from airflow.utils.decorators import apply_defaults
from airflow_kubernetes_job_operator.kube_api import KubeResourceState, KubeLogApiEvent
from airflow_kubernetes_job_operator.utils import (
    to_kubernetes_valid_name,
)
from airflow_kubernetes_job_operator.utils import resolve_relative_path
from airflow_kubernetes_job_operator.job_runner import JobRunner, JobRunnerDeletePolicy
from airflow_kubernetes_job_operator.exceptions import KubernetesJobOperatorException
from airflow_kubernetes_job_operator.config import (
    DEFAULT_VALIDATE_BODY_ON_INIT,
    DEFAULT_EXECUTION_OBJECT_PATHS,
    DEFAULT_EXECTION_OBJECT,
    DEFAULT_DELETE_POLICY,
    DEFAULT_TASK_STARTUP_TIMEOUT,
    DEFAULT_KUBERNETES_MAX_RESOURCE_NAME_LENGTH,
    IS_AIRFLOW_ONE,
    AIRFLOW_MAJOR_VERSION,
)


# Base class is determined by the airflow version.
if IS_AIRFLOW_ONE:
    from airflow.operators import BaseOperator

    class KubernetesJobOperatorDefaultsBase(BaseOperator):
        @apply_defaults
        def __init__(self, **kwargs) -> None:
            super().__init__(**kwargs)


else:
    from airflow.models import BaseOperator

    class KubernetesJobOperatorDefaultsBase(BaseOperator):
        def __init__(self, **kwargs) -> None:
            super().__init__(**kwargs)


def xcom_value_parser(value: str) -> dict:
    value = value.strip()
    try:
        value: dict = json.loads(value)
        assert isinstance(value, dict), "Value must be a json object (dict)"
    except Exception as ex:
        raise KubernetesJobOperatorException(
            "XCom messages (with default parser) must be in json object format:", *ex.args
        )
    return value


class KubernetesJobOperator(KubernetesJobOperatorDefaultsBase):
    autogenerate_job_id_from_task_id: bool = True
    resolve_relative_path_callstack_offset: int = 0

    def __init__(
        self,
        task_id: str,
        command: List[str] = None,
        arguments: List[str] = None,
        image: str = None,
        namespace: str = None,
        envs: dict = None,
        body: Union[str, dict, List[dict]] = None,
        body_filepath: str = None,
        image_pull_policy: str = None,
        delete_policy: Union[str, JobRunnerDeletePolicy] = DEFAULT_DELETE_POLICY,
        in_cluster: bool = None,
        config_file: str = None,
        get_logs: bool = True,
        cluster_context: str = None,
        startup_timeout_seconds: float = DEFAULT_TASK_STARTUP_TIMEOUT,
        validate_body_on_init: bool = DEFAULT_VALIDATE_BODY_ON_INIT,
        enable_jinja: bool = True,
        jinja_job_args: dict = None,
        on_kube_api_event: callable = None,
        parse_xcom_event: xcom_value_parser = xcom_value_parser,
        **kwargs,
    ):
        """A operator that executes an airflow task as a kubernetes Job.
        See: https://kubernetes.io/docs/concepts/workloads/controllers/jobs-run-to-completion/
        for notes about a kubernetes job.

        Keyword Arguments:

            command {List[str]} -- The pod main container command (default: None)
            arguments {List[str]} -- the pod main container arguments. (default: None)
            image {str} -- The image to use in the pod. (default: None)
            namespace {str} -- The namespace to execute in. (default: None)
            envs {dict} -= A collection of environment variables that will be added to all
                containers.
            body {dict|string} -- The job to execute as a yaml description. (default: None)
                If None, will use a default job yaml command. In this case you must provide an
                image.
            body_filepath {str} -- The path to the file to read the yaml from, overridden by
                body. (default: None)
            delete_policy {str} -- Any of: Never, Always, IfSucceeded (default: {"IfSucceeded"})
            in_cluster {bool} -- True if running inside a cluster (on a pod) (default: {False})
            config_file {str} -- The kubernetes configuration file to load, if
                None use default config. (default: {None})
            cluster_context {str} -- The context to run in, if None, use current context
                (default: {None})
            validate_body_on_init {bool} -- If true, validates the yaml in the constructor,
                setting this to True, will slow dag creation.
                (default: {from env/airflow config: AIRFLOW__KUBE_JOB_OPERATOR__validate_body_on_init or False})
            enable_jinja {bool} -- If true, the following fields will be parsed as jinja2,
                        command, arguments, image, envs, body, namespace, config_file, cluster_context
            jinja_job_args {dict} -- A dictionary or object to be used in the jinja template to render
                arguments. The jinja args are loaded under the keyword "job".
            on_kube_api_event {callable, optional} -- a method to catch api events when called. lambda api_event, context: ...
            parse_xcom_event {xcom_value_parser, optional} -- parse an incoming xcom event value.
                Must return a dictionary with key/value pairs.

        Auto completed yaml values (if missing):
            All:
                metadata.namespace = current namespace
            Pod:
                spec.restartPolicy = Never
            Job:
                spec.backOffLimit = 0
                spec.template.spec.restartPolicy = Never
                metadata.finalizers - [foregroundDeletion]

        """
        super().__init__(task_id=task_id, **kwargs)

        assert body_filepath is not None or body is not None or image is not None, ValueError(
            "body is None, body_filepath is None and an image was not defined. Unknown image to execute."
        )

        body = body or self._read_body_from_file(
            resolve_relative_path(
                body_filepath or DEFAULT_EXECUTION_OBJECT_PATHS[DEFAULT_EXECTION_OBJECT],
                self.resolve_relative_path_callstack_offset + (2 if AIRFLOW_MAJOR_VERSION > 1 else 1),
            )
        )

        assert body is not None and (isinstance(body, (dict, str))), ValueError(
            "body must either be a yaml string or a dict"
        )

        if isinstance(delete_policy, str):
            try:
                delete_policy = JobRunnerDeletePolicy(delete_policy)
            except Exception:
                delete_policy = None

        assert delete_policy is not None and isinstance(delete_policy, JobRunnerDeletePolicy), ValueError(
            f"Invalid delete policy. Valid values are ({JobRunnerDeletePolicy.__module__}.JobRunnerDeletePolicy):"
            + f" {[str(v) for v in JobRunnerDeletePolicy]}"
        )

        assert envs is None or isinstance(envs, dict), ValueError("The env collection must be a dict or None")
        assert image is None or isinstance(image, str), ValueError("image must be a string or None")

        # Job properties.
        self._job_is_executing = False
        self._job_runner: JobRunner = None

        # override/replace properties
        self.command = command
        self.arguments = arguments
        self.image = image
        self.envs = envs
        self.image_pull_policy = image_pull_policy
        self.body = body
        self.namespace = namespace
        self.get_logs = get_logs
        self.on_kube_api_event = on_kube_api_event
        self.parse_xcom_event = parse_xcom_event
        self.delete_policy = delete_policy

        # kubernetes config properties.
        self.config_file = config_file
        self.cluster_context = cluster_context
        self.in_cluster = in_cluster

        # operation properties
        self.startup_timeout_seconds = startup_timeout_seconds

        # Jinja
        self.jinja_job_args = jinja_job_args
        if enable_jinja:
            self.template_fields = [
                "command",
                "arguments",
                "image",
                "envs",
                "body",
                "namespace",
                "config_file",
                "cluster_context",
            ]

        # Used for debugging
        self._internal_wait_kuberentes_object_timeout = None

        if validate_body_on_init:
            assert not enable_jinja or isinstance(body, dict), ValueError(
                "Cannot set validate_body_on_init=True, if input body is string. "
                + "Jinja context only exists when the task is executed."
            )
            self.prepare_and_update_body()

    @classmethod
    def _read_body_from_file(cls, filepath):
        body = ""
        with open(filepath, "r", encoding="utf-8") as reader:
            body = reader.read()
        return body

    @property
    def job_runner(self) -> JobRunner:
        return self._job_runner

    @classmethod
    def _create_job_name(cls, name):
        return to_kubernetes_valid_name(
            name,
            max_length=DEFAULT_KUBERNETES_MAX_RESOURCE_NAME_LENGTH,
        )

    @classmethod
    def _to_kubernetes_env_list(cls, envs: dict):
        return [{"name": k, "value": f"{envs[k]}"} for k in envs.keys()]

    def _get_kubernetes_env_list(self):
        return self._to_kubernetes_env_list(self.envs or {})

    def _get_kubernetes_job_operator_envs(self):
        body = self.job_runner.body
        names = [r.get("metadata", {}).get("name", None) for r in body]
        names = [n for n in names if n is not None]
        return self._to_kubernetes_env_list(
            {
                "KUBERNETES_JOB_OPERATOR_RESOURCES": " ".join(names),
            }
        )

    def _update_container_yaml(self, container):
        container["env"] = [
            *self._get_kubernetes_job_operator_envs(),
            *container.get("env", []),
            *self._get_kubernetes_env_list(),
        ]

    def _update_main_container_yaml(self, container: dict):
        if self.command:
            container["command"] = self.command
        if self.arguments:
            container["args"] = self.arguments
        if self.image:
            container["image"] = self.image
        if self.image_pull_policy:
            container["imagePullPolicy"] = self.image_pull_policy

    def _update_override_params(self, o: dict):
        if "spec" in o and "containers" in o.get("spec", {}):
            containers: List[dict] = o["spec"]["containers"]
            if isinstance(containers, list) and len(containers) > 0:
                for container in containers:
                    self._update_container_yaml(container=container)
                self._update_main_container_yaml(containers[0])

                ## adding env resources
        for c in o.values():
            if isinstance(c, dict):
                self._update_override_params(c)

    def _validate_job_runner(self):
        if self._job_runner is not None:
            return
        self._job_runner = self.create_job_runner()
        assert isinstance(self._job_runner, JobRunner), KubernetesJobOperatorException(
            "create_job_runner method must return a value of type JobRunner"
        )

    def create_job_runner(self) -> JobRunner:
        """Override this method to create your own or augment the job runner"""
        # create the job runner.
        return JobRunner(
            body=self.body,
            namespace=self.namespace,
            show_pod_logs=self.get_logs,
            delete_policy=self.delete_policy,
            logger=self.logger if hasattr(self, "logger") else None,
            auto_load_kube_config=True,
            name_prefix=self._create_job_name(self.task_id),
        )

    def get_template_env(self) -> jinja2.Environment:
        """Creates the jinja environment for the template rendering.

        Returns:
            jinja2.Environment: The generated jinja environment.
        """
        jinja_env = super().get_template_env()
        jinja_env.globals["job"] = self.jinja_job_args or {}
        return jinja_env

    def prepare_and_update_body(self):
        """Call to prepare the body for execution, this is a heavy command."""
        self._validate_job_runner()
        self.job_runner.prepare_body()

    def pre_execute(self, context):
        """Called before execution by the airflow system.
        Overriding this method without calling its super() will
        break the job operator.

        Arguments:
            context -- The airflow context
        """
        self._validate_job_runner()
        self._job_is_executing = False

        # Load the configuration.
        self.job_runner.client.load_kube_config(
            config_file=self.config_file,
            is_in_cluster=self.in_cluster,
            context=self.cluster_context,
        )

        # prepare the body
        self.prepare_and_update_body()

        # write override params
        self._update_override_params(self.job_runner.body[0])

        # call parent.
        return super().pre_execute(context)

    def handle_kube_api_event(self, event: KubeLogApiEvent, context):
        if self.on_kube_api_event:
            self.on_kube_api_event(event, context)

        if event.name == "xcom":
            values_dict = self.parse_xcom_event(event.value or "{}")
            has_been_updated = False
            for key in values_dict.keys():
                self.xcom_push(
                    context=context,
                    key=key,
                    value=values_dict.get(key),
                )
                has_been_updated = True
            if has_been_updated:
                self.log.info(f"XCom updated, keys: " + ", ".join(values_dict.keys()))

    def execute(self, context):
        """Call to execute the kubernetes job.

        Arguments:
            context -- The airflow job.

        Raises:
            AirflowException: Error in execution.
        """
        assert self._job_runner is not None, KubernetesJobOperatorException(
            "Execute called without pre_execute. Job runner object was not created."
        )
        self._job_is_executing = True
        try:

            rslt = self.job_runner.execute_job(
                watcher_start_timeout=self.startup_timeout_seconds,
                timeout=self._internal_wait_kuberentes_object_timeout,
                on_kube_api_event=lambda event: self.handle_kube_api_event(
                    event=event,
                    context=context,
                ),
            )

            if rslt == KubeResourceState.Failed:
                raise KubernetesJobOperatorException(
                    f"Task {self.task_id} failed. See log for kubernetes execution details."
                )
        finally:
            self._job_is_executing = False

    def on_kill(self):
        """Called when the task is killed, either by
        making it as failed or when the operator finishes.
        """
        if self._job_is_executing:
            self.log.info(
                f"Task {self.task_id} killed/aborted while waiting for execution in kubernetes."
                + " Stopping and deleting job..."
            )
            try:
                self.job_runner.abort()
            except Exception:
                self.log.error("Failed to delete an aborted/killed" + " job! The job may still be executing.")

        return super().on_kill()
