from airflow_kubernetes_job_operator.kube_api.watchers import KubeApiNamespaceWatcher, KubeApiNamespaceWatcherKind


class KubeApiResourcesWatcher(EventHandler):
    _resource_yaml: str = None
    _log_reader: ThreadedKubernetesWatchPodLog = None
    _id: str = None
    _has_read_logs = False
    client: kubernetes.client.CoreV1Api = None
    _was_deleted: bool = False

    def __init__(
        self,
        client: kubernetes.client.CoreV1Api,
        resource_yaml: dict,
        auto_watch_pod_logs: bool = True,
    ):
        """A namespace resource state holder, with an internal
        log reader for pods.

        Do not create an instance of this class directly, use the
        ThreadedKubernetesNamespaceResourcesWatcher to watch for resource
        changes in the namespace.

        Arguments:

            client {kubernetes.client.CoreV1Api} -- The api client.
            resource_yaml {dict} -- The resource yaml description.

        Keyword Arguments:

            auto_watch_pod_logs {bool} -- [description] (default: {True})
        """
        super().__init__()
        self.client = client
        self._id = ThreadedKubernetesResourcesWatcher.compose_resource_id_from_yaml(resource_yaml)
        self.auto_watch_pod_logs = auto_watch_pod_logs
        self._was_deleted = False

    def emit(self, name, *args, **kwargs):
        """Emits the event to all the event handler
        callable(s). Any argument sent after name, will
        be passed to the event handler.

        Arguments:

            name {str} -- The name of the event to emit.
        """
        if len(args) < 2:
            args = list(args) + [self]
        super().emit(name, *args, **kwargs)

    def finalize(self):
        """Call to clean up any leftover state changes of the current watcher
        like reading missed loggs and clearing threads.
        """
        if self.kind == "Pod" and not self._has_read_logs:
            self._read_pod_log(False)

    def stop(self):
        """Stop all executing internal watchers."""
        if self._log_reader is not None and self._log_reader.is_streaming:
            self._log_reader.stop()

    @property
    def id(self) -> str:
        """The watcher id, composed by the current kuberentes
        path and values. May contain chars: a-z0-9/-.

        Returns:

            str
        """
        return self._id

    @property
    def kind(self) -> str:
        """The resource kind

        Returns:

            str -- May be any of the kubernetes resources,
            i.e. "Pod, Deployment, Job, Service ..."
        """
        return self.yaml["kind"]

    @property
    def yaml(self) -> dict:
        """The full resource yaml as dictionary.
        This property updates on cluster changes.

        Returns:

            dict -- The yaml as dict
        """
        return self._resource_yaml

    @property
    def name(self) -> str:
        """The resource name"""
        return self.yaml["metadata"]["name"]

    @property
    def namespace(self) -> str:
        """The resource namespace."""
        return self.yaml["metadata"]["namespace"]

    @staticmethod
    def compose_resource_id_from_yaml(resource_yaml: dict) -> str:
        """Returns the resource composed id from the resource yaml
        definition.

        Arguments:

            resource_yaml {dict} -- The resource yaml

        Returns:

            str -- The resource id.
        """
        return ThreadedKubernetesResourcesWatcher.compose_resource_id_from_values(
            resource_yaml["kind"],
            resource_yaml["metadata"]["namespace"],
            resource_yaml["metadata"]["name"],
        )

    @staticmethod
    def compose_resource_id_from_values(kind, namespace, name) -> str:
        """Composes an resource id given the specific required values.

        Arguments:

            kind {str} -- The resource kind
            namespace {str} -- The resource namespace
            name {str} -- The resource name

        Returns:

            str -- The resource id.
        """
        return "/".join([kind, namespace, name])

    @staticmethod
    def read_job_status_from_yaml(status_yaml: dict, backoffLimit: int = 0) -> str:
        """Returns a single job status value, after
        taking into account the description in the status yaml.

        Arguments:

            status_yaml {dict} -- The job status yaml dict.

        Keyword Arguments:

            backoffLimit {int} -- The job backoff limit (can be read
            from the job yaml) (default: {0})

        Returns:

            str -- The inferred job status. Can be any of: Pending, Succeeded, Failed, Running
        """
        job_status = "Pending"
        if "startTime" in status_yaml:
            if "completionTime" in status_yaml:
                job_status = "Succeeded"
            elif "failed" in status_yaml and int(status_yaml["failed"]) > backoffLimit:
                job_status = "Failed"
            else:
                job_status = "Running"
        return job_status

    @property
    def status(self) -> str:
        """Returns the inferred status of the current resource.

        Raises:
            Exception: [description]

        Returns:

            str -- The resource status. Can be any of:
                Pending - The resource task has not started yet.
                Running - The task is currently running.
                Succeeded - The resource completed its task successfully
                Failed - The resource received an error and is considered Failed.
                Deleted - Was deleted (not is terminating)
                Active - (in case the resource is definition "like", for example Service)
        """
        if self.kind == "Service":
            return "Deleted" if self._was_deleted else "Active"
        elif self.kind == "Job":
            job_status = None
            if self._was_deleted:
                job_status = "Deleted"
            else:
                job_status = ThreadedKubernetesResourcesWatcher.read_job_status_from_yaml(
                    self.yaml["status"], int(self.yaml["spec"]["backoffLimit"])
                )

            return job_status
        elif self.kind == "Pod":
            pod_status = None
            if self._was_deleted:
                pod_status = "Deleted"
            else:
                pod_status = self.yaml["status"]["phase"]
            if "status" in self.yaml and "containerStatuses" in self.yaml["status"]:
                for container_status in self.yaml["status"]["containerStatuses"]:
                    if "state" in container_status:
                        if (
                            "waiting" in container_status["state"]
                            and "reason" in container_status["state"]["waiting"]
                            and "BackOff" in container_status["state"]["waiting"]["reason"]
                        ):
                            return "Failed"
                        if "error" in container_status["state"]:
                            return "Failed"

            return pod_status
        else:
            raise Exception("Not implemented for kind: " + self.kind)

    def _update_pod_state(self, event_type: str, old_status: str):
        """FOR INTERNAL USE ONLY. Called to update
        in the case of a kind=Pod. Will trigger log reading if
        that is possible.

        Arguments:

            event_type {str} -- The update event type
            old_status {str} -- The previous pod status, or None.
        """
        # Called to update a current pod state.
        # may start/read pod logs.
        cur_status = self.status
        need_read_pod_logs = cur_status and cur_status != "Pending" and not self._has_read_logs

        if need_read_pod_logs:
            self._has_read_logs = True
            self._read_pod_log(self.status == "Running")

    def _read_pod_log(self, run_async: bool):
        """Call to read the pod logs, or start stream reading.

        Arguments:
            run_async {bool} -- If true do async stream reading.
        """
        # may be called my multiple threads.
        pod_log_read_lock = threading.Lock()
        try:
            pod_log_read_lock.acquire()

            if self._log_reader is None:
                self._log_reader = ThreadedKubernetesWatchPodLog()
                self._log_reader.pipe(self)

            if self._log_reader.is_streaming:
                return

            if run_async:
                self._log_reader.start(self.client, self.name, self.namespace)
            else:
                self._log_reader.read_currnet_logs(self.client, self.name, self.namespace)
        finally:
            pod_log_read_lock.release()

    def update_resource_state(self, event_type: str, resource_yaml: dict):
        """Call to update a resource state to match the resource_yaml
        description. Will trigger reader watchers if needed.

        Arguments:

            event_type {str} -- The event type.
            resource_yaml {dict} -- The resource yaml description.
        """

        # can me multi threaded.
        lock = threading.Lock()
        try:
            lock.acquire()

            is_new = self._resource_yaml is None
            old_status = None if is_new else self.status

            # update the current yaml.
            self._resource_yaml = resource_yaml

            if not self._was_deleted:
                self._was_deleted = event_type == "DELETED"

            if self.kind == "Pod":
                self._update_pod_state(event_type, old_status)

            if old_status != self.status:
                self.emit("status", self.status, self)
        finally:
            lock.release()
