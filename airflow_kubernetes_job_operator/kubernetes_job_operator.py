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
        name_prefix: str = None,
        name_postfix: str = None,
        random_name_postfix_length: int = 8,
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
        """A operator that executes an airflow task given a configuration yaml or as a kubernetes job.
        See: https://kubernetes.io/docs/concepts/workloads/controllers/jobs-run-to-completion/
        for notes about a kubernetes job.

        Args:
            task_id (str): The airflow task id.
            command (List[str], optional): The kubernetes pod command. Defaults to None.
            arguments (List[str], optional): The kubernetes pod arguments. Defaults to None.
            image (str, optional): The kubernetes container image to use. Defaults to None.
            name_prefix (str, optional): The kubernetes resource(s) name prefix. Defaults to (corrected) task_id.
            name_postfix (str, optional): The kuberntes resource(s) name postfix. Defaults to None.
            random_name_postfix_length (int, optional): Add a random string to all kuberntes resource(s) names if > 0.
                Defaults to 8.
            namespace (str, optional): The kubernetes namespace to run in. Defaults to current namespace.
            envs (dict, optional): A dictionary of key value pairs that is loaded into the environment variables.
                Defaults to None.
            body (Union[str, dict, List[dict]], optional):
                The dictionary/list[dictionary] or yaml that defines the job yaml.
                None = use KubernetesJobOperator default job yaml
                    (https://github.com/LamaAni/KubernetesJobOperator/blob/master/airflow_kubernetes_job_operator/templates/job_default.yaml)
            body_filepath (str, optional): A filepath to the job yaml or json configuration. Can be a relative path.
                Defaults to None -> use body.
            image_pull_policy (str, optional): The kubernetes image pull policy. Defaults to None.
            delete_policy (Union[str, JobRunnerDeletePolicy], optional): The delete policy to use.
                e.g. What to do when the task finishes. Defaults to DEFAULT_DELETE_POLICY.
            in_cluster (bool, optional): If true, currently running in cluster. Defaults to autodetect.
            config_file (str, optional): The kube connection configuration file. Defaults to autodetect.
            get_logs (bool, optional): Read the logs from all the resources in the body. Defaults to True.
            cluster_context (str, optional): The name of the cluster to use. Defaults to autodetect.
            startup_timeout_seconds (float, optional): A timeout for the any of the pods to start.
                Defaults to DEFAULT_TASK_STARTUP_TIMEOUT.
            enable_jinja (bool, optional): If true enable jinja in args. Defaults to True.
            jinja_job_args (dict, optional): An key value pair argument list to be added on top
                of airflow argument list. Defaults to None.

        Advanced:
            validate_body_on_init (bool, optional): If true, validates the body before the
                operator is executed in the worker. Defaults to DEFAULT_VALIDATE_BODY_ON_INIT.
            on_kube_api_event (callable, optional): Called when the kube api emits an event (Pending, Running).
                Defaults to None.
            parse_xcom_event (xcom_value_parser, optional): Called when an xcom event is detected to parse it.
                Defaults to xcom_value_parser.
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
        self.name_prefix = name_prefix
        self.name_postfix = name_postfix
        self.random_name_postfix_length = random_name_postfix_length
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
                "name_prefix",
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
    def _create_kubernetes_job_name_prefix(cls, name):
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

                # adding env resources
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
            name_prefix=self._create_kubernetes_job_name_prefix(
                self.name_prefix if self.name_prefix is not None else self.task_id
            ),
            name_postfix=self.name_postfix,
            random_name_postfix_length=self.random_name_postfix_length,
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
                self.log.info("XCom updated, keys: " + ", ".join(values_dict.keys()))

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
