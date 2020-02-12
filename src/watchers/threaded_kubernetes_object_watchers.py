import kubernetes
from typing import Dict, List
from queue import SimpleQueue
from .event_handler import EventHandler
from .threaded_kubernetes_watch import (
    ThreadedKubernetesWatchNamspeace,
    ThreadedKubernetesWatchPodLog,
    ThreadedKubernetesWatch,
)


class ThreadedKubernetesObjectsWatcher(EventHandler):
    _object_yaml: str = None
    auto_watch_pod_logs: bool = True
    _log_reader: ThreadedKubernetesWatchPodLog = None
    _id: str = None
    _has_read_logs = False
    client: kubernetes.client.CoreV1Api = None
    _was_deleted: bool = False

    def __init__(
        self,
        client: kubernetes.client.CoreV1Api,
        object_yaml: dict,
        auto_watch_pod_logs: bool = True,
    ):
        """A namespace object state holder, with an internal
        log reader for pods.
        
        Do not create this object directly, use the 
        ThreadedKubernetesNamespaceObjectsWatcher to watch for object
        changes in the namespace.
        
        Arguments:

            client {kubernetes.client.CoreV1Api} -- The api client.
            object_yaml {dict} -- The object yaml description.
        
        Keyword Arguments:

            auto_watch_pod_logs {bool} -- [description] (default: {True})
        """
        super().__init__()
        self.client = client
        self._id = ThreadedKubernetesObjectsWatcher.compose_object_id_from_yaml(object_yaml)
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

    def stop(self):
        """Stop all executing internal watchers.
        """
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
        """The object kind
        
        Returns:

            str -- May be any of the kubernetes resources, 
            i.e. "Pod, Deployment, Job, Service ..."
        """
        return self.yaml["kind"]

    @property
    def yaml(self) -> dict:
        """The full object yaml as dictionary.
        This property updates on cluster changes.
        
        Returns:

            dict -- The yaml as dict
        """
        return self._object_yaml

    @property
    def name(self) -> str:
        """The object name
        """
        return self.yaml["metadata"]["name"]

    @property
    def namespace(self) -> str:
        """The object namespace.
        """
        return self.yaml["metadata"]["namespace"]

    @staticmethod
    def compose_object_id_from_yaml(object_yaml: dict) -> str:
        """Returns the object composed id from the object yaml
        definition.
        
        Arguments:

            object_yaml {dict} -- The object yaml
        
        Returns:

            str -- The object id.
        """
        return ThreadedKubernetesObjectsWatcher.compose_object_id_from_values(
            object_yaml["kind"],
            object_yaml["metadata"]["namespace"],
            object_yaml["metadata"]["name"],
        )

    @staticmethod
    def compose_object_id_from_values(kind, namespace, name) -> str:
        """Composes an object id given the specific required values.
        
        Arguments:

            kind {str} -- The object kind
            namespace {str} -- The object namespace
            name {str} -- The object name
        
        Returns:

            str -- The object id.
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
        """Returns the inferred status of the current object.
        
        Raises:
            Exception: [description]
        
        Returns:
        
            str -- The object status. Can be any of: 
                Pending - The object task has not started yet.
                Running - The task is currently running.
                Succeeded - The object completed its task successfully
                Failed - The object received an error and is considered Failed.
                Deleted - Was deleted (not is terminating)
                Active - (in case the object is definition "like", for example Service)
        """
        if self.kind == "Service":
            return "Deleted" if self._was_deleted else "Active"
        elif self.kind == "Job":
            job_status = None
            if self._was_deleted:
                job_status = "Deleted"
            else:
                job_status = ThreadedKubernetesObjectsWatcher.read_job_status_from_yaml(
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
        read_pod_static_logs = self.status != "Running"

        if need_read_pod_logs:
            self._log_reader = ThreadedKubernetesWatchPodLog()
            self._log_reader.pipe(self)
            self._has_read_logs = True
            if read_pod_static_logs:
                # in case where the logs were too fast,
                # and they need to be read sync.
                self._log_reader.read_currnet_logs(self.client, self.name, self.namespace)
                self._log_reader = None
                pass
            else:
                # async read logs.
                self._log_reader.start(self.client, self.name, self.namespace)

    def update_object_state(self, event_type: str, object_yaml: dict):
        """Call to update an object state to match the object_yaml
        description. Will trigger reader watchers if needed.
        
        Arguments:
        
            event_type {str} -- The event type.
            object_yaml {dict} -- The object yaml description.
        """
        is_new = self._object_yaml is None
        old_status = None if is_new else self.status

        # update the current yaml.
        self._object_yaml = object_yaml

        if not self._was_deleted:
            self._was_deleted = event_type == "DELETED"

        if self.kind == "Pod":
            self._update_pod_state(event_type, old_status)

        if old_status != self.status:
            self.emit("status", self.status, self)


class ThreadedKubernetesNamespaceObjectsWatcher(EventHandler):
    _object_watchers: Dict[str, ThreadedKubernetesObjectsWatcher] = None
    _namespace_watchers: Dict[str, List[ThreadedKubernetesWatch]] = None
    auto_watch_pod_logs: bool = True
    remove_deleted_kube_objects_from_memory: bool = True
    client: kubernetes.client.CoreV1Api = None

    def __init__(self, client: kubernetes.client.CoreV1Api):
        """A namespace object (resource) watcher. Allows for multiple namespaces
        and multiple filtering values.
        
        Arguments:

            client {kubernetes.client.CoreV1Api} -- The kubernetes client.
        """
        super().__init__()
        self.client = client
        self._object_watchers = dict()
        self._namespace_watchers = dict()

    @property
    def object_watchers(self) -> Dict[str, ThreadedKubernetesObjectsWatcher]:
        """The collection of object watchers associated
        with this namespace watcher.
        
        Returns:

            Dict[str, ThreadedKubernetesObjectsWatcher]
        """
        return self._object_watchers

    def watch_namespace(
        self, namespace: str, label_selector: str = None, field_selector: str = None
    ):
        """Add a watch condition on namespace.
        
        Arguments:

            namespace {str} -- The namespace
        
        Keyword Arguments:

            label_selector {str} -- The label selector to filter objects (default: {None})
            field_selector {str} -- The field selector to filter objects (default: {None})
        """
        assert isinstance(namespace, str) and len(namespace) > 0
        if namespace in self._namespace_watchers:
            raise Exception("Namespace already being watched.")

        watchers = []
        self._namespace_watchers[namespace] = watchers
        for kind in ["Pod", "Job", "Deployment", "Service"]:
            watcher = ThreadedKubernetesWatchNamspeace()
            watcher.pipe(self)
            watcher.start(
                client=self.client,
                namespace=namespace,
                kind=kind,
                field_selector=field_selector,
                label_selector=label_selector,
            )
            watchers.append(watcher)

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
        if name == "DELETED":
            self._delete_object(args[0])
        elif name == "update":
            self._update_object(args[0])

        if len(args) < 2:
            args = list(args) + [self]

        super().emit(name, *args, **kwargs)

    def _update_object(self, event: dict):
        """FOR INTERNAL USE ONLY.
        Updates a matched object watcher with the yaml events.
        Will create object watchers if needed.
        
        Arguments:
        
            event {dict} -- The kubernetes event dict.
        """
        kube_object = event["object"]
        event_type = event["type"]
        oid = ThreadedKubernetesObjectsWatcher.compose_object_id_from_yaml(kube_object)
        if oid not in self._object_watchers:
            if event_type == "DELETED":
                return
            self._object_watchers[oid] = ThreadedKubernetesObjectsWatcher(
                self.client, kube_object, self.auto_watch_pod_logs
            )
            self._object_watchers[oid].pipe(self)

        self._object_watchers[oid].update_object_state(event_type, kube_object)

    def _delete_object(self, event):
        """FOR INTERNAL USE ONLY.
        Deletes a matched object watcher with the yaml events.
        
        Arguments:

            event {dict} -- The kubernetes event dict.
        """
        # first update the current object.
        self._update_object(event)

        kube_object = event["object"]
        oid = ThreadedKubernetesObjectsWatcher.compose_object_id_from_yaml(kube_object)
        if oid in self._object_watchers:
            watcher = self._object_watchers[oid]
            watcher.stop()
            if self.remove_deleted_kube_objects_from_memory:
                del self._object_watchers[oid]

    def waitfor(
        self,
        predict: callable,
        include_log_events: bool = False,
        timeout: float = None,
        event_name: str = "update",
    ) -> ThreadedKubernetesObjectsWatcher:
        """A thread blocker method to wait for 
        a condition. This method will wait until
        the result of the predict callable is true.
        
        Arguments:

            predict {callable(*args,**kwargs)} -- A method to predict
            when to stop waiting. This method accepts the arguments as
            they were sent to the event.
        
        Keyword Arguments:

            include_log_events {bool} -- [description] (default: {False})
            timeout {float} -- If specified, the timeout to wait until
            throwing an error (default: {None})
            event_name {str} -- The name of the event to wait for (default: {"update"})
        
        Returns:

            ThreadedKubernetesObjectsWatcher -- The associated object
            which fulfilled the wait for event.
        """

        class wait_event:
            args: list = None
            kwargs: dict = None

            def __init__(self, args, kwargs):
                super().__init__()
                self.args = args
                self.kwargs = kwargs

        event_queue = SimpleQueue()

        def add_queue_event(*args, **kwargs):
            event_queue.put(wait_event(list(args), dict(kwargs)))

        event_handler_idx = self.on(event_name, add_queue_event)

        info = None
        while True:
            info = event_queue.get(timeout=timeout)
            args = info.args
            kwargs = info.kwargs
            if predict(*args, **kwargs):
                break

        self.clear(event_name, event_handler_idx)
        return None if info is None else info.args[-1]

    def waitfor_status(
        self,
        kind: str = None,
        name: str = None,
        namespace: str = None,
        status: str = None,
        status_list: List[str] = None,
        predict: callable = None,
        timeout: float = None,
        check_current_status: bool = True,
    ) -> ThreadedKubernetesObjectsWatcher:
        """Block the thread and wait for a valid status event.
        
        Keyword Arguments:

            kind {str} -- The object kind filter. (default: {None})
            name {str} -- The object name filter. (default: {None})
            namespace {str} -- The object namespace filter. (default: {None})
            status {str} -- A status to wait for (default: {None})
            status_list {List[str]} -- A list of status values to wait for (default: {None})
            predict {callable} -- A predict(status,*args,**kwargs) method
            to determine when the condition has been met. (default: {None})
            timeout {float} -- Timeout to wait. (default: {None})
            check_current_status {bool} -- If true, then will also check current status.
            (default: {True})
        
        Returns:

            ThreadedKubernetesObjectsWatcher -- The object watcher.
        """
        assert (
            status is not None
            or (status_list is not None and len(status_list) > 0)
            or predict is not None
        )

        def default_predict(match_status: str, sender: ThreadedKubernetesObjectsWatcher):
            if status is not None and status == match_status:
                return True
            if status_list is not None:
                for sli in status_list:
                    if sli == match_status:
                        return True
            return False

        predict = predict or default_predict

        def wait_predict(status, sender: ThreadedKubernetesObjectsWatcher):
            if name is not None and sender.name != name:
                return False
            if namespace is not None and sender.namespace != namespace:
                return False
            if kind is not None and sender.kind != kind:
                return False
            return predict(status, sender)

        # check if need past events to be loaded.
        if check_current_status:
            for sender in self._object_watchers.values():
                if wait_predict(sender.status, sender):
                    return sender

        return self.waitfor(wait_predict, False, timeout=timeout, event_type="status")

    def stop(self):
        """Stop all executing watchers.
        """
        for namespace, watchers in self._namespace_watchers.items():
            for watcher in watchers:
                watcher.stop()

        for oid, owatch in self._object_watchers.items():
            owatch.stop()
