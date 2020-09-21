import os
from typing import List, Union

from airflow import configuration
from airflow import AirflowException
from airflow.utils.decorators import apply_defaults
from airflow.operators import BaseOperator
from airflow_kubernetes_job_operator.kube_api import KubeObjectState
from airflow_kubernetes_job_operator.utils import (
    to_kubernetes_valid_name,
)
from airflow_kubernetes_job_operator.job_runner import JobRunner


class KubernetesJobOperatorException(AirflowException):
    pass


KUBERNETES_JOB_OPERATOR_DEFAULT_BODY = os.path.abspath(f"{__file__}.default.yaml")


class KubernetesJobOperator(BaseOperator):
    autogenerate_job_id_from_task_id: bool = True

    @apply_defaults
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
        delete_policy: str = "IfSucceeded",
        in_cluster: bool = None,
        config_file: str = None,
        get_logs: bool = True,
        cluster_context: str = None,
        startup_timeout_seconds: int = None,
        validate_body_on_init: bool = configuration.conf.getboolean(
            "kube_job_operator", "validate_body_on_init", fallback=False
        )
        or False,
        *args,
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

        Auto completed yaml values (if missing):

            metadata.namespace - current namespace
            spec.backOffLimit - 0
            spec.template.spec.restartPolicy - Never

        Added yaml values:

            metadata.finalizers += foregroundDeletion

        """
        super().__init__(task_id, *args, **kwargs)

        assert body is not None or image is not None, ValueError(
            "body is None, and an image was not defined. Unknown image to execute."
        )

        body = body or self._read_body(body_filepath or KUBERNETES_JOB_OPERATOR_DEFAULT_BODY)

        assert body is not None and (isinstance(body, (dict, str))), ValueError(
            "body must either be a yaml string or a dict"
        )

        assert delete_policy is not None and delete_policy.lower() in [
            "never",
            "always",
            "ifsucceeded",
        ], "the delete_policy must be one of: Never, Always, IfSucceeded"

        assert envs is None or isinstance(envs, dict), ValueError("The env collection must be a dict or None")
        assert image is None or isinstance(image, str), ValueError("image must be a string or None")

        self._job_is_executing = False

        # override/replace properties
        self.command = command
        self.arguments = arguments
        self.image = image
        self.envs = envs
        self.image_pull_policy = image_pull_policy

        # kubernetes config properties.
        self.config_file = config_file
        self.cluster_context = cluster_context
        self.in_cluster = in_cluster

        # operation properties
        self.startup_timeout_seconds = startup_timeout_seconds
        self.delete_policy = delete_policy
        self.get_logs = get_logs

        # create the job runner.
        self.job_runner: JobRunner = JobRunner(
            body=body,
            logger=self.logger,
            namespace=namespace,
            auto_load_kube_config=False,
            name_postfix=self._create_job_name(),
            show_pod_logs=get_logs,
        )

        if validate_body_on_init:
            self.prepare_and_update_body()

    @staticmethod
    def _read_body(filepath):
        body = ""
        with open(filepath, "r", encoding="utf-8") as reader:
            body = reader.read()
        return body

    @property
    def body(self) -> dict:
        return self.job_runner.body

    def _create_job_name(self):
        return to_kubernetes_valid_name(
            self.task_id,
            max_length=configuration.conf.getint("kube_job_operator", "max_job_name_length", fallback=50),
        )

    def prepare_and_update_body(self):
        """Call to prepare the body for execution, this is a heavy command."""
        self.job_runner.prepare_body()

        def update_override_params(o: dict):
            if "spec" in o and "containers" in o.get("spec", {}):
                containers: List[dict] = o["spec"]["containers"]
                if isinstance(containers, list) and len(containers) > 0:
                    main_container = containers[0]
                    if self.command:
                        main_container["command"] = self.command
                    if self.arguments:
                        main_container["args"] = self.arguments
                    if self.envs:
                        envs = main_container.get("envs", [])
                        for k in self.envs.keys():
                            envs.append({"name": k, "value": self.envs[k]})
                        main_container["envs"] = envs
                    if self.image:
                        main_container["image"] = self.image
                    if self.image_pull_policy:
                        main_container["imagePullPolicy"] = self.image_pull_policy
            for c in o.values():
                if isinstance(c, dict):
                    update_override_params(c)

        update_override_params(self.job_runner.body[0])

    def pre_execute(self, context):
        """Called before execution by the airflow system.
        Overriding this method without calling its super() will
        break the job operator.

        Arguments:
            context -- The airflow context
        """
        self._job_is_executing = False

        # Load the configuration.
        self.job_runner.client.load_kube_config(
            config_file=self.config_file,
            is_in_cluster=self.in_cluster,
            context=self.cluster_context,
        )

        # updating parameters and overrides.
        self.prepare_and_update_body()

        # call parent.
        return super().pre_execute(context)

    def execute(self, context):
        """Call to execute the kubernetes job.

        Arguments:
            context -- The airflow job.

        Raises:
            AirflowException: Error in execution.
        """
        delete_policy = self.delete_policy.lower()
        self.job_runner.delete_on_failure = delete_policy == "always"
        self.job_runner.delete_on_success = self.job_runner.delete_on_failure or delete_policy == "ifsucceeded"

        self._job_is_executing = True
        try:
            rslt = self.job_runner.execute_job(watcher_start_timeout=self.startup_timeout_seconds)

            if rslt == KubeObjectState.Failed:
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
