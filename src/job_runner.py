import kubernetes
import yaml
import copy
from .watchers.threaded_kubernetes_object_watchers import (
    ThreadedKubernetesNamespaceObjectsWatcher,
    ThreadedKubernetesObjectsWatcher
)
from .watchers.event_handler import EventHandler
from .utils import randomString, get_from_dictionary_path

JOB_RUNNER_INSTANCE_ID_LABEL = "job-runner-instance-id"


class JobRunner(EventHandler):
    batchClient: kubernetes.client.BatchV1Api
    _id: str = None

    def __init__(self):
        """Creates a job runner object that can be used
        to execute a job.

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

    def prepare_job_yaml(
            self,
            job_yaml,
            random_name_postfix_length: int = 0
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

        Returns:
            dict -- The prepared job yaml dictionary.
        """
        # make sure the yaml is an object.
        job_yaml = copy.deepcopy(job_yaml) if isinstance(job_yaml, dict) \
            else yaml.safe_load(job_yaml)

        def get(path_names, default=None):
            try:
                return get_from_dictionary_path(job_yaml, path_names)
            except Exception as e:
                if default:
                    return default
                raise Exception("Error reading yaml: "+str(e)) from e

        def assert_defined(path_names: list, def_name=None):
            path_string = '.'.join(map(lambda v: str(v), path_names))
            assert get(path_names) is not None,\
                f"job {def_name or path_names[-1]} must be defined @ {path_string}"

        assert get(["kind"]) == "Job",\
            "job_yaml object 'kind' must be of kind job, recived " + \
            get(["kind"], "[null]")

        assert_defined(["metadata", "name"])
        assert_defined(["metadata", "namespace"])
        assert_defined(["spec", "template"])
        assert_defined(
            ["spec", "template", "spec", "containers", 0],
            "main container"
        )

        if random_name_postfix_length > 0:
            job_yaml["metadata"]["name"] += '-' + \
                randomString(random_name_postfix_length)

        # FIXME: Should be a better way to add missing values.
        if "labels" not in job_yaml["metadata"]:
            job_yaml["metadata"]["labels"] = dict()

        if "metadata" not in job_yaml["spec"]["template"]:
            job_yaml["spec"]["template"]["metadata"] = dict()

        if "labels" not in job_yaml["spec"]["template"]["metadata"]:
            job_yaml["spec"]["template"]["metadata"]["labels"] = dict()

        instance_id = randomString(15)
        job_yaml["metadata"]["labels"][JOB_RUNNER_INSTANCE_ID_LABEL] = instance_id
        job_yaml["spec"]["template"]["metadata"]["labels"][JOB_RUNNER_INSTANCE_ID_LABEL] = instance_id

        return job_yaml

    def execute_job(
        self,
        job_yaml: dict
    ) -> ThreadedKubernetesObjectsWatcher:
        """Executes a job with a pre-prepared job yaml,
        to prepare the job yaml please call JobRunner.prepare_job_yaml

        Arguments:
            job_yaml {dict} -- The dictionary of the job body.
            Can only have one kubernetes element.

        Raises:
            Exception: [description]

        Returns:
            ThreadedKubernetesObjectsWatcher -- The run result
            as a watch object, holds the final yaml information
            and the object status.
        """

        assert "metadata" in job_yaml \
            and "labels" in job_yaml["metadata"]\
            and JOB_RUNNER_INSTANCE_ID_LABEL in job_yaml["metadata"]["labels"],\
            "job_yaml is not configured correctly, " +\
            "did you forget to call JobRunner.prepare_job_yaml?"

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
            raise Exception(
                f"Job {name} already exists in namespace {namespace}, cannot exec.")

        # starting the watcher.
        watcher = ThreadedKubernetesNamespaceObjectsWatcher(coreClient)
        watcher.watch_namespace(
            namespace,
            label_selector=f"{JOB_RUNNER_INSTANCE_ID_LABEL}={instance_id}"
        )
        watcher.pipe(self)

        # starting the job
        batchClient.create_namespaced_job(namespace, job_yaml)
        job_watch_object = watcher.waitfor_status(
            "Job", name, namespace, status="Running")

        self.emit("job_started", job_watch_object, self)

        # waiting for the job to completed.
        job_watch_object = watcher.waitfor_status(
            "Job", name, namespace, status_list=["Failed", "Succeeded"])

        # not need to read status and logs anymore.
        watcher.stop()

        if job_watch_object.status == "Failed":
            self.emit("job_failed", job_watch_object, self)

        return job_watch_object
