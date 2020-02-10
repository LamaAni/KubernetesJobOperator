import os
import yaml
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException
from airflow.operators import BaseOperator
from .utils import to_kubernetes_valid_name

# from airflow.operators.bash_operator import BashOperator
# from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
# from airflow.contrib.kubernetes import kube_client
from .job_runner import JobRunner
from .watchers.threaded_kubernetes_object_watchers import (
    ThreadedKubernetesObjectsWatcher,
    ThreadedKubernetesNamespaceObjectsWatcher,
)

# FIXME: To be moved to airflow config.
DEFAULT_VALIDATE_YAML_ON_INIT = (
    os.environ.get("AIRFLOW__KUBE_JOB_OPERATOR__VALIDATE_YAML_ON_INIT", "false").lower()
    == "true"
)

# FIXME: To be moved to airflow config.
MAX_JOB_NAME_LENGTH = int(
    os.environ.get("AIRFLOW__KUBE_JOB_OPERATOR__MAX_JOB_NAME_LENGTH", "50")
)


class KubernetesBaseJobOperator(BaseOperator):
    job_yaml: dict = None
    job_runner: JobRunner = None
    in_cluster: bool = False
    config_file: str = None
    cluster_context: str = None
    job_name_random_postfix_length: int = 5
    autogenerate_job_id_from_task_id: bool = True
    __waiting_for_job_execution: bool = False

    # Can be any of: IfSucceeded, Always, Never
    delete_policy: str = "IfSucceeded"

    @apply_defaults
    def __init__(
        self,
        job_yaml,
        delete_policy: str = "IfSucceeded",
        in_cluster: bool = False,
        config_file: str = None,
        cluster_context: str = None,
        validate_yaml_on_init: bool = DEFAULT_VALIDATE_YAML_ON_INIT,
        *args,
        **kwargs,
    ):
        super(KubernetesBaseJobOperator, self).__init__(*args, **kwargs)

        assert job_yaml is not None and (
            isinstance(job_yaml, (dict, str))
        ), "job_yaml must either be a string yaml or a dict object"

        assert delete_policy is not None and delete_policy.lower() in [
            "never",
            "always",
            "ifsucceeded",
        ], "the delete_policy must be one of: Never, Always, IfSucceeded"

        self.job_yaml = job_yaml
        self.config_file = config_file
        self.cluster_context = cluster_context
        self.in_cluster = in_cluster
        self.delete_policy = delete_policy

        # create the job runner.
        self.job_runner = JobRunner()
        self.job_runner.on("log", lambda msg, sender: self.on_job_log(msg, sender))
        self.job_runner.on(
            "status",
            lambda status, sender: self.on_job_object_status_changed(status, sender),
        )

        if validate_yaml_on_init:
            self.job_runner.prepare_job_yaml(self.job_yaml)

    def create_job_name(self):
        return to_kubernetes_valid_name(self.task_id, max_length=MAX_JOB_NAME_LENGTH)

    def on_job_log(self, msg, sender: ThreadedKubernetesObjectsWatcher):
        self.log.info(f"{sender.id}: {msg}")

    def on_job_object_status_changed(
        self, status, sender: ThreadedKubernetesObjectsWatcher
    ):
        self.log.info(f"{sender.id} ({status})")

    def log_final_result(
        self,
        job_watcher: ThreadedKubernetesObjectsWatcher,
        namespace_watcher: ThreadedKubernetesNamespaceObjectsWatcher,
    ):
        if job_watcher.status in ["Failed", "Deleted"]:
            pod_count = len(
                list(
                    filter(
                        lambda ow: ow.kind == "Pod",
                        namespace_watcher.object_watchers.values(),
                    )
                )
            )
            self.log.error(f"Job Failed ({pod_count} pods), last pod/job status:")

            # log proper object error
            def log_object_error(object_watcher: ThreadedKubernetesObjectsWatcher):
                log_method = (
                    self.log.error
                    if object_watcher.status == "Failed"
                    else self.log.info
                )
                log_method(
                    "FINAL STATUS: "
                    + object_watcher.id
                    + f" ({object_watcher.status})\n"
                    + yaml.dump(object_watcher.yaml["status"])
                )

            for object_watcher in namespace_watcher.object_watchers.values():
                log_object_error(object_watcher)
        else:
            self.log.info("Job complete.")

    def pre_execute(self, context):
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
            force_job_name=job_name,
        )

        # call parent.
        return super().pre_execute(context)

    def execute(self, context):
        self.log.info("Starting job...")
        self.__waiting_for_job_execution = True

        # Executing the job
        (job_watcher, namespace_watcher) = self.job_runner.execute_job(self.job_yaml)

        self.__waiting_for_job_execution = False
        self.log_final_result(job_watcher, namespace_watcher)

        # Check delete policy.
        delete_policy = self.delete_policy.lower()
        if delete_policy == "always" or (
            delete_policy == "ifsucceeded" and job_watcher.status == "Succeeded"
        ):
            self.log.info("Deleting job leftovers")
            self.job_runner.delete_job(job_watcher.yaml)
        else:
            self.log.warning("Job object(s) left in namespace")

        if job_watcher.status != "Succeeded":
            raise AirflowException(f"Job {job_watcher.status}")

    def on_kill(self):
        if self.__waiting_for_job_execution:
            self.log.info(
                f"Job killed/aborted while waiting for execution to complete. Deleting job..."
            )
            self.job_runner.delete_job(self.job_yaml)
            self.log.info("Job deleted.")

        return super().on_kill()
