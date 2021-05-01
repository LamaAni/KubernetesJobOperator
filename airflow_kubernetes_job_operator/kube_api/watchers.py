import json
from datetime import datetime
from logging import Logger
from weakref import WeakSet, WeakValueDictionary
from typing import List, Dict, Tuple, Union
from zthreading.events import EventHandler, Event
from zthreading.tasks import Task

from airflow_kubernetes_job_operator.kube_api.utils import kube_logger
from airflow_kubernetes_job_operator.kube_api.exceptions import KubeApiException
from airflow_kubernetes_job_operator.kube_api.collections import (
    KubeResourceState,
    KubeResourceKind,
    KubeApiRestQueryConnectionState,
)
from airflow_kubernetes_job_operator.kube_api.client import KubeApiRestQuery, KubeApiRestClient
from airflow_kubernetes_job_operator.kube_api.queries import (
    GetNamespaceResources,
    GetPodLogs,
    KubeLogApiEvent,
    LogLine,
)

from zthreading.decorators import thread_synchronized


class NamespaceWatchQueryResourceState(EventHandler):
    def __init__(
        self,
        uid: str,
        status_changed_event_name="status_changed",
        state_changed_event_name="state_changed",
        deleted_event_name="deleted",
    ) -> None:
        """Maintains the state information about a kubernetes resources.

        This object is created and used internally.

        Args:
            uid (str): The kubernetes resource uid.
            status_changed_event_name (str, optional): The event name for status changed. Defaults to "status_changed".
            state_changed_event_name (str, optional): The event name for state changed. Defaults to "state_changed".
            deleted_event_name (str, optional): The event name for deleted. Defaults to "deleted".
        """
        super().__init__()
        self.body = {"metadata": {"uid": uid}}
        self._deleted = False
        self.status_changed_event_name = status_changed_event_name
        self.deleted_event_name = deleted_event_name
        self.state_changed_event_name = state_changed_event_name
        self._last_state: KubeResourceState = None
        self._state: KubeResourceState = None
        self._kind: KubeResourceKind = None

    @classmethod
    def get_identifiers(cls, yaml: dict) -> Tuple[str, str]:
        """Returns the kind and the uid of a resource"""
        uid = yaml.get("metadata", {}).get("uid", None)
        kind = yaml.get("kind", "[unknown kube element]").lower()
        return uid, kind

    @property
    def uid(self) -> str:
        return self.metadata["uid"]

    @property
    def kind(self) -> KubeResourceKind:
        if self._kind is None:
            if "kind" not in self.body:
                return None
            self._kind = KubeResourceKind.create_from_existing(
                self.body.get("kind", "{undefined}"),
                self.body.get("apiVersion", None),
            )
        return self._kind

    @property
    def kind_name(self) -> str:
        return self.kind.name

    @property
    def name(self) -> str:
        return self.metadata.get("name", None)

    @property
    def namespace(self) -> str:
        return self.metadata.get("namespace", None)

    @property
    def deleted(self) -> bool:
        return self._deleted

    @property
    def metadata(self) -> dict:
        return self.body.get("metadata", {})

    @property
    def status(self) -> dict:
        return self.body.get("status", {})

    @property
    def state(self) -> KubeResourceState:
        if self._state is None:
            if self.kind is None:
                return None
            self._state = self.kind.parse_state(self.body, self.deleted)
        return self._state

    def update(self, body: dict):
        """Updates the object state information given its (new) body.

        Args:
            object_yaml (dict): The object body.
        """
        if body.get("event_type", None) == "DELETED":
            self._deleted = True
            self.emit(self.deleted_event_name)

        has_status_changed = json.dumps(self.status) != json.dumps(body.get("status", {}))
        if has_status_changed:
            self.emit(self.status_changed_event_name)

        self.body.update(body)

        # reset the state.
        self._state = None

        if self._last_state != self.state:
            self.emit(self.state_changed_event_name)
        self._last_state = self.state


class NamespaceWatchQuery(KubeApiRestQuery):
    status_changed_event_name = "status_changed"
    state_changed_event_name = "state_changed"
    deleted_event_name = "deleted"
    watch_started_event_name = "watch_started"
    pod_logs_reader_started_event_name = "pod_logs_reader_started"
    pod_log_api_event_name = GetPodLogs.kube_api_event_name

    def __init__(
        self,
        kinds: list = None,
        namespace: Union[str, List[str]] = None,
        label_selector: str = None,
        field_selector: str = None,
        watch_pod_logs: bool = True,
        timeout: float = None,
        collect_resource_state: bool = True,
        pod_log_event_name: str = "log",
        pod_log_since: datetime = None,
    ):
        """A namespace watcher that tracks the state and logs of namespace(s) elements.

        Args:
            kinds (list, optional): The object kinds to watch. If None defaults to KubeResourceKind.watchable()
            . Defaults to None.
            namespace (Union[str, List[str]], optional): A namespace or list of namespaces to watch
            . Defaults to None.
            label_selector (str, optional): Kubernetes label selector. Defaults to None.
            field_selector (str, optional): Kubernetes field selector to filter resources. Defaults to None.
            watch_pod_logs (bool, optional): If true, also follow all pod logs. Defaults to True.
            timeout (float, optional): Startup timeout. Defaults to None.
            collect_resource_state (bool, optional): If true, collects and tracks the resources state. Defaults to True.
            pod_log_event_name (str, optional): Event name for logging. Defaults to "log".
            pod_log_since (datetime, optional): Watch pod logs since. See GetPodLogs. Defaults to None.
        """
        super().__init__(
            None,
            method="GET",
            timeout=timeout,
        )

        # update kinds
        kinds = kinds or KubeResourceKind.watchable()

        self.namespaces = [] if namespace is None else namespace if isinstance(namespace, list) else [namespace]
        self.label_selector = label_selector
        self.field_selector = field_selector
        self.watch_pod_logs = watch_pod_logs
        self.pod_log_event_name = pod_log_event_name
        self.pod_log_since = pod_log_since
        self.collect_resource_state = collect_resource_state

        self.kinds = {}
        for kind in kinds:
            kind: KubeResourceKind = kind if isinstance(kind, KubeResourceKind) else KubeResourceKind.get_kind(kind)
            self.kinds[kind.name] = kind

        self._executing_queries: List[KubeApiRestQuery] = WeakSet()  # type:ignore
        self._executing_pod_loggers: Dict[str, GetPodLogs] = WeakValueDictionary()  # type:ignore
        self._object_states: Dict[str, NamespaceWatchQueryResourceState] = dict()

    @property
    def watched_objects(self) -> List[NamespaceWatchQueryResourceState]:
        return list(self._object_states.values())

    def emit_log(self, data):
        self.emit(self.pod_log_event_name, data)

    def emit_api_event(self, api_event: KubeLogApiEvent):
        self.emit(self.pod_log_api_event_name, api_event)

    @thread_synchronized
    def _create_pod_log_reader(
        self,
        logger_id: str,
        name: str,
        namespace: str,
        container: str = None,
        follow=True,
        is_single=False,
    ) -> GetPodLogs:
        read_logs = GetPodLogs(
            name=name,
            namespace=namespace,
            since=self.pod_log_since,
            follow=follow,
            container=container,
            add_container_name_to_log=False if is_single else True,
        )

        self._executing_pod_loggers[logger_id] = read_logs
        return read_logs

    def process_data_state(self, data: dict, client: KubeApiRestClient):
        if not self.is_running:
            return
        uid, kind = NamespaceWatchQueryResourceState.get_identifiers(data)
        if self.collect_resource_state and kind in self.kinds:
            if uid not in self._object_states:
                self._object_states[uid] = NamespaceWatchQueryResourceState(
                    uid,
                    self.status_changed_event_name,
                    self.state_changed_event_name,
                    self.deleted_event_name,
                )
                self._object_states[uid].pipe(self)

            state = self._object_states[uid]
            state.update(data)
            if state.deleted:
                del self._object_states[uid]

        if self.watch_pod_logs and kind == "pod":
            name = data["metadata"]["name"]
            namesoace = data["metadata"]["namespace"]
            pod_status = data["status"]["phase"]

            if pod_status != "Pending":
                containers = data["spec"]["containers"]
                is_single = len(containers) < 2
                for container in containers:
                    if not isinstance(container, dict):
                        continue

                    container_name = container.get("name", None)

                    assert isinstance(container_name, str) and len(container_name.strip()) > 0, KubeApiException(
                        "Invalid container name when reading logs"
                    )

                    logger_id = f"{uid}/{container_name}"

                    if logger_id in self._executing_pod_loggers:
                        continue

                    osw = self._object_states.get(uid)
                    read_logs: GetPodLogs = self._create_pod_log_reader(
                        logger_id=logger_id,
                        name=name,
                        namespace=namesoace,
                        container=container.get("name", None),
                        is_single=is_single,
                    )

                    osw.emit(self.pod_logs_reader_started_event_name, container=container_name)

                    def handle_error(sender, *args):
                        # Don't throw error if not running.
                        if not self.is_running:
                            return

                        if len(args) == 0:
                            self.emit_error(KubeApiException("Unknown error from sender", sender))
                        else:
                            self.emit_error(args[0])

                    # binding only relevant events.
                    read_logs.on(read_logs.data_event_name, lambda line: self.emit_log(line))
                    read_logs.on(
                        read_logs.kube_api_event_name, lambda api_event: self.emit_api_event(api_event=api_event)
                    )
                    read_logs.on(read_logs.error_event_name, handle_error)
                    client.query_async(read_logs)

    def _stop_all_loggers(
        self,
        timeout: float = None,
        throw_error_if_not_running: bool = None,
    ):
        for pod_logger in list(self._executing_pod_loggers.values()):
            pod_logger.stop(timeout=timeout, throw_error_if_not_running=throw_error_if_not_running)

    def stop(
        self,
        timeout: float = None,
        throw_error_if_not_running: bool = None,
    ):
        for q in self._executing_queries:
            q.stop(timeout=timeout, throw_error_if_not_running=throw_error_if_not_running)

        self._stop_all_loggers(
            timeout=timeout,
            throw_error_if_not_running=throw_error_if_not_running,
        )

        return super().stop(timeout=timeout, throw_error_if_not_running=throw_error_if_not_running)

    def log_event(self, logger: Logger, ev: Event):
        if ev.name == self.query_before_reconnect_event_name:
            if isinstance(ev.sender, GetNamespaceResources):
                get_ns_objs: GetNamespaceResources = ev.sender
                logger.info(
                    f"[{get_ns_objs.namespace}/{get_ns_objs.kind.plural}] "
                    + f"Watch collection for {get_ns_objs.kind.plural} lost, attempting to reconnect..."
                )
        elif ev.name == self.state_changed_event_name:
            osw: NamespaceWatchQueryResourceState = ev.sender
            logger.info(f"[{osw.namespace}/{osw.kind_name.lower()}s/{osw.name}]" + f" {osw.state}")
        elif ev.name == self.pod_log_event_name:
            line: LogLine = ev.args[0]
            line.log(logger)
        elif ev.name == self.pod_logs_reader_started_event_name:
            osw: NamespaceWatchQueryResourceState = ev.sender
            container_name = ev.kwargs.get("container", "[unknown container name]")
            logger.info(f"[{osw.namespace}/{osw.kind_name.lower()}s/{osw.name}] Reading logs from {container_name}")

    def pipe_to_logger(self, logger: Logger = kube_logger, allowed_event_names=None) -> int:
        allowed_event_names = set(
            allowed_event_names
            or [
                self.pod_logs_reader_started_event_name,
                self.state_changed_event_name,
                self.pod_log_event_name,
            ]
        )

        return super().pipe_to_logger(logger, allowed_event_names)

    def wait_for_state(
        self,
        state: Union[KubeResourceState, List[KubeResourceState]],
        kind: KubeResourceKind,
        name: str,
        namespace: str,
        timeout: float = None,
    ) -> KubeResourceState:
        """Wait for a resource to reach a state. Dose not look in past events.

        Args:
            state (Union[KubeResourceState, List[KubeResourceState]]): The state to wait for.
            kind (KubeResourceKind): The resource kind.
            name (str): The resource name
            namespace (str): The resource namespace.
            timeout (float, optional): Wait timeout. Defaults to None.

        Returns:
            KubeResourceState: The final resource state.
        """
        if not isinstance(state, list):
            state = [state]

        final_state = None

        def state_reached(sender: EventHandler, event: Event):
            if event.name != self.state_changed_event_name:
                return False
            if not isinstance(event.sender, NamespaceWatchQueryResourceState):
                return False

            osw: NamespaceWatchQueryResourceState = event.sender
            if osw.name != name or osw.namespace != namespace or osw.kind.name != kind.name:
                return False

            if osw.state in state:
                nonlocal final_state
                final_state = osw.state
                return True
            else:
                return False

        self.wait_for(predict=state_reached, raise_errors=True, timeout=timeout)
        return final_state

    def query_loop(self, client: KubeApiRestClient):
        # specialized loop. Uses the event handler to read multiple sourced events,
        # and waits for the stream to stop.
        queries: List[GetNamespaceResources] = []
        namespaces = set(self.namespaces)
        if len(namespaces) == 0:
            namespaces = set([client.get_default_namespace()])

        for namespace in namespaces:
            for kind in list(self.kinds.values()):
                q = GetNamespaceResources(
                    kind=kind,
                    namespace=namespace,
                    watch=True,
                    field_selector=self.field_selector,
                    label_selector=self.label_selector,
                )
                q.on(q.data_event_name, lambda data: self.process_data_state(data, client))
                q.pipe(self)

                queries.append(q)
                self._executing_queries.add(q)

        self._set_connection_state(KubeApiRestQueryConnectionState.Connecting)

        # Starting the queries.
        client.query_async(queries)
        for q in queries:
            q.wait_until_running(timeout=None)

        # State changed to running.
        self._set_connection_state(KubeApiRestQueryConnectionState.Streaming)
        Task.wait_for_all(queries)

        # Stopping any leftover loggers.
        self._stop_all_loggers(timeout=self.timeout, throw_error_if_not_running=False)
