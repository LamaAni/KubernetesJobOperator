from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException
from airflow.operators import BaseOperator
from airflow.operators.bash_operator import BashOperator

# from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
# from airflow.contrib.kubernetes import kube_client
from .job_runner import JobRunner
from .watchers.threaded_kubernetes_object_watchers import (
    ThreadedKubernetesObjectsWatcher,
)


class KubernetesBaseJobOperator(BaseOperator):
    job_yaml: dict = None
    job_runner: JobRunner = None
    in_cluster: bool = True
    config_file: str = None
    cluster_context: str = None
    job_name_random_postfix_length: int = 5

    # Can be any of: IfFailed, Always, Never
    delete_policy: str = "IfFailed"

    @apply_defaults
    def __init__(
        self,
        job_yaml,
        delete_policy: str = "IfFailed",
        in_cluster: bool = True,
        config_file: str = None,
        cluster_context: str = None,
        *args,
        **kwargs,
    ):
        super(KubernetesBaseJobOperator, self).__init__(*args, **kwargs)

        assert job_yaml is not None and (
            isinstance(job_yaml, (dict, str))
        ), "job_yaml must either be a string yaml or a dict object"

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

    def on_job_log(self, msg, sender: ThreadedKubernetesObjectsWatcher):
        self.log.info(f"{sender.id}: {msg}")

    def on_job_object_status_changed(
        self, status, sender: ThreadedKubernetesObjectsWatcher
    ):
        self.log.info(f"{sender.id} ({status})")

    def log_final_result(self, job_watcher: ThreadedKubernetesObjectsWatcher):
        if job_watcher.status == "Failed":
            self.log.error(job_watcher.yaml["status"])
            self.log.error("Job Failed.")
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

        # fromat the yaml to the expected values of the job.
        # override this method to allow pre/post formatting
        # of the yaml dictionary.
        self.job_yaml = self.job_runner.prepare_job_yaml(
            self.job_yaml, self.job_name_random_postfix_length
        )

        # call parent.
        return super().pre_execute(context)

    def execute(self, context):

        self.log.info("Starting job...")

        # Executing the job
        (job_watcher, watcher) = self.job_runner.execute_job(self.job_yaml)

        self.log_final_result(job_watcher)

        if job_watcher.status != "Succeeded":
            raise AirflowException("Job failed")
