import kubernetes
import yaml
import copy
import os
from .watchers.threaded_kubernetes_resource_watchers import (
    ThreadedKubernetesNamespaceResourcesWatcher,
    ThreadedKubernetesResourcesWatcher,
)
from .watchers.event_handler import EventHandler
from .utils import randomString, get_from_dictionary_path

JOB_RUNNER_INSTANCE_ID_LABEL = "job-runner-instance-id"


class JobRunner(EventHandler):
    def __init__(self):
        """Holds methods for executing jobs on a cluster.

        Example:

            runner = JobRunner()
            runner.on("log", lambda msg, sender: print(msg))
            jobyaml = runner.prepare_job_yaml(
                '...',
                random_name_postfix_length=5
            )
            info = runner.execute_job(jobyaml)
            print(info.status)
            print(info.yaml)
        """
        super().__init__()

    def load_kuberntes_configuration(
        self, in_cluster: bool = False, config_file: str = None, context: str = None
    ):
        """Loads the appropriate kubernetes configuration into the global
        context.
        
        Keyword Arguments:

            in_cluster {bool} -- If true, load the configuration from the cluster
                (default: {False})
            config_file {str} -- the path to the file to load from,
                if None, loads the kubernetes default ~/.kube. (default: {None})
            context {str} -- The context to load. If None loads the current
                context (default: {None})
        """
        # loading the current config to use.
        if in_cluster:
            kubernetes.config.load_incluster_config()
        else:
            # load from file must have a config file,
            assert config_file is None or os.path.exists(
                config_file
            ), f"Cannot find kubernetes configuration file @ {config_file}"

            kubernetes.config.load_kube_config(config_file=config_file, context=context)

    def prepare_job_yaml(
        self, job_yaml, random_name_postfix_length: int = 0, force_job_name: str = None
    ) -> dict:
        """Pre-prepare the job yaml dictionary for execution,
        can also accept a string input.

        Arguments:

            job_yaml {dict|str} -- The job yaml, either a string
                or a dictionary.

        Keyword Arguments:

            random_name_postfix_length {int} -- The number of random
                characters to add to job name, if 0 then do not add.
                allow for the rapid generation of random jobs. (default: {0})
            force_job_name {str} -- If exist will replace the job name.

        Auto completed yaml values (if missing):

            metadata.namespace - current namespace
            spec.backOffLimit - 0
            spec.template.spec.restartPolicy - Never

        Added yaml values:

            metadata.finalizers += foregroundDeletion

        Returns:

            dict -- The prepared job yaml dictionary.
        """
        if (
            isinstance(job_yaml, dict)
            and "metadata" in job_yaml
            and "labels" in job_yaml["metadata"]
            and JOB_RUNNER_INSTANCE_ID_LABEL in job_yaml["metadata"]["labels"]
        ):
            # already initialized.
            return

        # make sure the yaml is an dict.
        job_yaml = (
            copy.deepcopy(job_yaml) if isinstance(job_yaml, dict) else yaml.safe_load(job_yaml)
        )

        def get(path_names, default=None):
            try:
                return get_from_dictionary_path(job_yaml, path_names)
            except Exception as e:
                if default:
                    return default
                raise Exception("Error reading yaml: " + str(e)) from e

        def assert_defined(path_names: list, def_name=None):
            path_string = ".".join(map(lambda v: str(v), path_names))
            assert (
                get(path_names) is not None
            ), f"job {def_name or path_names[-1]} must be defined @ {path_string}"

        assert get(["kind"]) == "Job", "job_yaml resource must be of 'kind' 'Job', recived " + get(
            ["kind"], "[unknown]"
        )

        assert_defined(["metadata", "name"])
        assert_defined(["spec", "template"])
        assert_defined(["spec", "template", "spec", "containers", 0], "main container")

        if force_job_name is not None:
            job_yaml["metadata"]["name"] = force_job_name

        if random_name_postfix_length > 0:
            job_yaml["metadata"]["name"] += "-" + randomString(random_name_postfix_length)

        # assign current namespace if one is not defined.
        if "namespace" not in job_yaml["metadata"]:
            contexts, active_context = kubernetes.config.list_kube_config_contexts()
            current_namespace = active_context["context"]["namespace"]
            job_yaml["metadata"]["namespace"] = current_namespace

        # FIXME: Should be a better way to add missing values.
        if "labels" not in job_yaml["metadata"]:
            job_yaml["metadata"]["labels"] = dict()

        if "backoffLimit" not in job_yaml["spec"]:
            job_yaml["spec"]["backoffLimit"] = 0

        if "metadata" not in job_yaml["spec"]["template"]:
            job_yaml["spec"]["template"]["metadata"] = dict()

        if "labels" not in job_yaml["spec"]["template"]["metadata"]:
            job_yaml["spec"]["template"]["metadata"]["labels"] = dict()

        if "finalizers" not in job_yaml["metadata"]:
            job_yaml["metadata"]["finalizers"] = []

        if "foregroundDeletion" not in set(job_yaml["metadata"]["finalizers"]):
            job_yaml["metadata"]["finalizers"].append("foregroundDeletion")

        if "restartPolicy" not in job_yaml["spec"]["template"]["spec"]:
            job_yaml["spec"]["template"]["spec"]["restartPolicy"] = "Never"

        instance_id = randomString(15)
        job_yaml["metadata"]["labels"][JOB_RUNNER_INSTANCE_ID_LABEL] = instance_id
        job_yaml["spec"]["template"]["metadata"]["labels"][
            JOB_RUNNER_INSTANCE_ID_LABEL
        ] = instance_id

        return job_yaml

    def execute_job(
        self, job_yaml: dict
    ) -> (ThreadedKubernetesResourcesWatcher, ThreadedKubernetesNamespaceResourcesWatcher):
        """Executes a job with a pre-prepared job yaml,
        to prepare the job yaml please call JobRunner.prepare_job_yaml

        Arguments:

            job_yaml {dict} -- The dictionary of the job body.
            Can only have one kubernetes element.

        Raises:

            Exception: [description]

        Returns:

            ThreadedKubernetesResourcesWatcher -- The run result
            as a watch dict, holds the final yaml information
            and the resource status.
        """

        assert (
            "metadata" in job_yaml
            and "labels" in job_yaml["metadata"]
            and JOB_RUNNER_INSTANCE_ID_LABEL in job_yaml["metadata"]["labels"]
        ), (
            "job_yaml is not configured correctly, "
            + "did you forget to call JobRunner.prepare_job_yaml?"
        )

        metadata = job_yaml["metadata"]
        name = metadata["name"]
        namespace = metadata["namespace"]
        instance_id = metadata["labels"][JOB_RUNNER_INSTANCE_ID_LABEL]

        # creating the client
        coreClient = kubernetes.client.CoreV1Api()
        batchClient = kubernetes.client.BatchV1Api()

        # checking if job exists.
        status = None
        try:
            status = batchClient.read_namespaced_job_status(name, namespace)
        except Exception:
            pass

        if status is not None:
            raise Exception(f"Job {name} already exists in namespace {namespace}, cannot exec.")

        # starting the watcher.
        watcher = ThreadedKubernetesNamespaceResourcesWatcher(coreClient)
        watcher.remove_deleted_kube_resources_from_memory = False
        watcher.pipe(self)
        watcher.watch_namespace(
            namespace,
            label_selector=f"{JOB_RUNNER_INSTANCE_ID_LABEL}={instance_id}",
            watch_for_kinds=["Job", "Pod"],
        )

        # starting the job
        batchClient.create_namespaced_job(namespace, job_yaml)

        # wait for job to start
        job_watcher = watcher.waitfor_status("Job", name, namespace, status="Running")
        self.emit("job_started", job_watcher, self)

        # waiting for the job to completed.
        job_watcher = watcher.waitfor_status(
            "Job", name, namespace, status_list=["Failed", "Succeeded", "Deleted"]
        )

        # not need to read status and logs anymore.
        watcher.stop()

        if job_watcher.status == "Failed":
            self.emit("job_failed", job_watcher, self)

        return job_watcher, watcher

    def delete_job(self, job_yaml: dict):
        """Using a pre-prepared job yaml, deletes a currently executing job.
        
        Arguments:
        
            job_yaml {dict} -- The job description yaml.
        """
        metadata = job_yaml["metadata"]
        name = metadata["name"]
        namespace = metadata["namespace"]

        batchClient = kubernetes.client.BatchV1Api()
        batchClient.delete_namespaced_job(name, namespace)
