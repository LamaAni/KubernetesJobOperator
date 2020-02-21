import os
import yaml
from typing import List

from airflow import configuration
from airflow.exceptions import AirflowException
from airflow.utils.decorators import apply_defaults
from airflow.operators import BaseOperator

from .utils import to_kubernetes_valid_name, set_yaml_path_value
from .job_runner import JobRunner
from .threaded_kubernetes_resource_watchers import (
    ThreadedKubernetesResourcesWatcher,
    ThreadedKubernetesNamespaceResourcesWatcher,
)

JOB_YAML_DEFAULT_FILE = os.path.abspath(f"{__file__}.default.yaml")


class KubernetesJobOperator(BaseOperator):
    job_yaml: dict = None
    job_runner: JobRunner = None
    in_cluster: bool = False
    config_file: str = None
    cluster_context: str = None
    job_name_random_postfix_length: int = 5
    startup_timeout_seconds: int = None
    autogenerate_job_id_from_task_id: bool = True
    __waiting_for_job_execution: bool = False

    # Can be any of: IfSucceeded, Always, Never
    delete_policy: str = "IfSucceeded"

    @apply_defaults
    def __init__(
        self,
        command: List[str] = None,
        arguments: List[str] = None,
        image: str = None,
        namespace: str = None,
        name: str = None,
        job_yaml=None,
        job_yaml_filepath=None,
        delete_policy: str = "IfSucceeded",
        in_cluster: bool = None,
        config_file: str = None,
        get_logs: bool = True,
        cluster_context: str = None,
        startup_timeout_seconds: int = None,
        validate_yaml_on_init: bool = configuration.conf.getboolean(
            "kube_job_operator", "VALIDATE_YAML_ON_INIT", fallback=False
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
            name {str} -- Override automatic name creation for the job. (default: None)
            job_yaml {dict|string} -- The job to execute as a yaml description. (default: None)
                If None, will use a default job yaml command. In this case you must provide an
                image.
            job_yaml_filepath {str} -- The path to the file to read the yaml from, overridden by 
                job_yaml. (default: None)
            delete_policy {str} -- Any of: Never, Always, IfSucceeded (default: {"IfSucceeded"})
            in_cluster {bool} -- True if running inside a cluster (on a pod) (default: {False})
            config_file {str} -- The kubernetes configuration file to load, if 
                None use default config. (default: {None})
            cluster_context {str} -- The context to run in, if None, use current context
                (default: {None})
            validate_yaml_on_init {bool} -- If true, validates the yaml in the constructor,
                setting this to True, will slow dag creation.
                (default: {from env/airflow config: AIRFLOW__KUBE_JOB_OPERATOR__VALIDATE_YAML_ON_INIT or False})

        Auto completed yaml values (if missing):

            metadata.namespace - current namespace
            spec.backOffLimit - 0
            spec.template.spec.restartPolicy - Never

        Added yaml values:

            metadata.finalizers += foregroundDeletion
        
        """
        super(KubernetesJobOperator, self).__init__(*args, **kwargs)

        assert (
            job_yaml is not None or image is not None
        ), "job_yaml is None, and an image was not defined. Unknown image to execute."

        # use or load
        job_yaml = job_yaml or self.read_job_yaml(job_yaml_filepath or JOB_YAML_DEFAULT_FILE)

        assert job_yaml is not None and (
            isinstance(job_yaml, (dict, str))
        ), "job_yaml must either be a string in yaml format or a dict"

        assert delete_policy is not None and delete_policy.lower() in [
            "never",
            "always",
            "ifsucceeded",
        ], "the delete_policy must be one of: Never, Always, IfSucceeded"

        # override/replace properties
        self.name = name
        self.namespace = namespace
        self.command = command
        self.arguments = arguments
        self.image = image

        # kubernetes config properties.
        self.job_yaml = job_yaml
        self.config_file = config_file
        self.cluster_context = cluster_context
        self.in_cluster = in_cluster

        # operation properties
        self.startup_timeout_seconds = startup_timeout_seconds
        self.delete_policy = delete_policy
        self.get_logs = get_logs

        # create the job runner.
        self.job_runner = JobRunner()
        self.job_runner.on("log", lambda msg, sender: self.on_job_log(msg, sender))
        self.job_runner.on(
            "status", lambda status, sender: self.on_job_status_changed(status, sender),
        )

        if validate_yaml_on_init:
            self.job_runner.prepare_job_yaml(self.job_yaml)

    @staticmethod
    def read_job_yaml(filepath):
        job_yaml = ""
        with open(filepath, "r", encoding="utf-8") as reader:
            job_yaml = reader.read()
        return job_yaml

    def create_job_name(self):
        """Create a name for the job, to be replaced with
        the name provided in the yaml. This method internally uses
        the method 'to_kubernetes_valid_name' in utils.

        Override this method to create or augment your own name.
        
        Returns:
            str -- The job name
        """
        return to_kubernetes_valid_name(
            self.task_id,
            max_length=configuration.conf.getboolean(
                "kube_job_operator", "MAX_JOB_NAME_LENGTH", fallback=False
            )
            or 50,
        )

    def on_job_log(self, msg: str, sender: ThreadedKubernetesResourcesWatcher):
        """Write a job log to the airflow logger. Override this method
        to handle the log format.
        
        Arguments:

            msg {str} -- The log message
            sender {ThreadedKubernetesResourcesWatcher} -- The kubernetes resource.
        """
        self.log.info(f"{sender.id}: {msg}")

    def on_job_status_changed(self, status, sender: ThreadedKubernetesResourcesWatcher):
        """Log the status changes of the kubernetes resources.
        
        Arguments:

            status {str} -- The status
            sender {ThreadedKubernetesResourcesWatcher} -- The kubernetes resource.
        """
        self.log.info(f"{sender.id} ({status})")

    def log_job_result(
        self,
        job_watcher: ThreadedKubernetesResourcesWatcher,
        namespace_watcher: ThreadedKubernetesNamespaceResourcesWatcher,
    ):
        """Log the results of the job to kubernetes.
        
        Arguments:

            job_watcher {ThreadedKubernetesResourcesWatcher} -- The kubernetes resource.
            namespace_watcher {ThreadedKubernetesNamespaceResourcesWatcher} -- The kubernetes
                namespace resources watcher, which holds all the job resources used.
        """
        if job_watcher.status in ["Failed", "Deleted"]:
            pod_count = len(
                list(
                    filter(
                        lambda resource_watcher: resource_watcher.kind == "Pod",
                        namespace_watcher.resource_watchers.values(),
                    )
                )
            )
            self.log.error(f"Job Failed ({pod_count} pods), last pod/job status:")

            # log proper resource error
            def log_resource_error(resource_watcher: ThreadedKubernetesResourcesWatcher):
                log_method = (
                    self.log.error if resource_watcher.status == "Failed" else self.log.info
                )
                log_method(
                    "FINAL STATUS: "
                    + resource_watcher.id
                    + f" ({resource_watcher.status})\n"
                    + yaml.dump(resource_watcher.yaml["status"])
                )

            for resource_watcher in namespace_watcher.resource_watchers.values():
                log_resource_error(resource_watcher)
        else:
            self.log.info("Job complete.")

    def pre_execute(self, context):
        """Called before execution by the airflow system.
        Overriding this method without calling its super() will
        break the job operator.
        
        Arguments:
            context -- The airflow context
        """
        # Load the configuration. NOTE: the configuration
        # should be only loaded while executing since
        # the executing enivroment can and will be different
        # then the airflow scheduler enviroment.
        self.job_runner.load_kuberntes_configuration(
            self.in_cluster, self.config_file, self.cluster_context
        )

        job_name = None
        if self.autogenerate_job_id_from_task_id:
            job_name = self.create_job_name()

        # fromat the yaml to the expected values of the job.
        # override this method to allow pre/post formatting
        # of the yaml dictionary.
        self.job_yaml = self.job_runner.prepare_job_yaml(
            self.job_yaml,
            random_name_postfix_length=self.job_name_random_postfix_length,
            force_job_name=self.name or job_name,
        )

        # updating override parameters (allow use default job yaml)
        def set_if_not_none(path_names: list, value):
            if value is None:
                return
            set_yaml_path_value(self.job_yaml, path_names, value)

        set_if_not_none(["metadata", "name"], self.name)
        set_if_not_none(["metadata", "namespace"], self.namespace)
        set_if_not_none(["spec", "template", "spec", "containers", 0, "command"], self.command)
        set_if_not_none(["spec", "template", "spec", "containers", 0, "args"], self.arguments)
        set_if_not_none(["spec", "template", "spec", "containers", 0, "image"], self.image)

        # call parent.
        return super().pre_execute(context)

    def execute(self, context):
        """Call to execute the kubernetes job.
        
        Arguments:
            context -- The airflow job.
        
        Raises:
            AirflowException: Error in execution.
        """
        self.log.info("Starting job...")
        self.__waiting_for_job_execution = True

        # Executing the job
        (job_watcher, namespace_watcher) = self.job_runner.execute_job(
            self.job_yaml, start_timeout=self.startup_timeout_seconds, read_logs=self.get_logs
        )

        self.__waiting_for_job_execution = False
        self.log_job_result(job_watcher, namespace_watcher)

        # Check delete policy.
        delete_policy = self.delete_policy.lower()
        if delete_policy == "always" or (
            delete_policy == "ifsucceeded" and job_watcher.status == "Succeeded"
        ):
            self.log.info("Deleting job leftovers")
            self.job_runner.delete_job(job_watcher.yaml)
        else:
            self.log.warning("Job resource(s) left in namespace")

        if job_watcher.status != "Succeeded":
            raise AirflowException(f"Job {job_watcher.status}")

    def on_kill(self):
        """Called when the task is killed, either by 
        making it as failed or when the operator finishes.
        """
        if self.__waiting_for_job_execution:
            self.log.info(
                f"Job killed/aborted while waiting for execution to complete. Deleting job..."
            )
            try:
                self.job_runner.delete_job(self.job_yaml)
                self.log.info("Job deleted.")
            except Exception:
                self.log.error(
                    "Failed to delete an aborted/killed" + " job! The job may still be executing."
                )

        return super().on_kill()
