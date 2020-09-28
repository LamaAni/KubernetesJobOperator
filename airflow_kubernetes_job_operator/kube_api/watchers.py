import json
from datetime import datetime
from logging import Logger
from weakref import WeakSet, WeakValueDictionary
from typing import List, Dict
from zthreading.events import EventHandler, Event
from zthreading.tasks import Task

from airflow_kubernetes_job_operator.kube_api.utils import kube_logger
from airflow_kubernetes_job_operator.kube_api.exceptions import KubeApiException
from airflow_kubernetes_job_operator.kube_api.collections import KubeObjectState, KubeObjectKind
from airflow_kubernetes_job_operator.kube_api.client import KubeApiRestQuery, KubeApiRestClient
from airflow_kubernetes_job_operator.kube_api.queries import (
    GetNamespaceObjects,
    GetPodLogs,
    LogLine,
)


class NamespaceWatchQueryObjectState(EventHandler):
    def __init__(
        self,
        uid: str,
        status_changed_event_name="status_changed",
        state_changed_event_name="state_changed",
        deleted_event_name="deleted",
    ) -> None:
        super().__init__()
        self.body = {"metadata": {"uid": uid}}
        self._deleted = False
        self.status_changed_event_name = status_changed_event_name
        self.deleted_event_name = deleted_event_name
        self.state_changed_event_name = state_changed_event_name
        self._last_state: KubeObjectState = None
        self._state: KubeObjectState = None
        self._kind: KubeObjectKind = None

    @classmethod
    def detect(cls, yaml: dict):
        uid = yaml.get("metadata", {}).get("uid", None)
        kind = yaml.get("kind", "[unknown kube element]").lower()
        return uid, kind

    @property
    def uid(self) -> str:
        return self.metadata["uid"]

    @property
    def kind(self) -> KubeObjectKind:
        if self._kind is None:
            if "kind" not in self.body:
                return None
            self._kind = KubeObjectKind.create_from_existing(
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
    def state(self) -> KubeObjectState:
        if self._state is None:
            if self.kind is None:
                return None
            self._state = self.kind.parse_state(self.body, self.deleted)
        return self._state

    def update(self, object_yaml):
        if object_yaml.get("event_type", None) == "DELETED":
            self._deleted = True
            self.emit(self.deleted_event_name)

        has_status_changed = json.dumps(self.status) != json.dumps(object_yaml.get("status", {}))
        if has_status_changed:
            self.emit(self.status_changed_event_name)

        self.body.update(object_yaml)

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

    def __init__(
        self,
        kinds: list = None,
        namespace: str = None,
        watch: bool = True,
        label_selector: str = None,
        field_selector: str = None,
        watch_pod_logs: bool = True,
        timeout: float = None,
        collect_kube_object_state: bool = True,
        pod_log_event_name: str = "log",
        pod_log_since: datetime = None,
    ):
        super().__init__(
            None,
            method="GET",
            timeout=timeout,
        )

        # update kinds
        kinds = KubeObjectKind.parseable()

        self.watch = watch
        self.namespaces = [] if namespace is None else namespace if isinstance(namespace, list) else [namespace]
        self.label_selector = label_selector
        self.field_selector = field_selector
        self.watch_pod_logs = watch_pod_logs
        self.pod_log_event_name = pod_log_event_name
        self.pod_log_since = pod_log_since
        self.collect_kube_object_state = collect_kube_object_state

        self.kinds = {}
        for kind in kinds:
            kind: KubeObjectKind = kind if isinstance(kind, KubeObjectKind) else KubeObjectKind.get_kind(kind)
            self.kinds[kind.name] = kind

        self._executing_queries: List[KubeApiRestQuery] = WeakSet()  # type:ignore
        self._executing_pod_loggers: Dict[str, GetPodLogs] = WeakValueDictionary()  # type:ignore
        self._object_states: Dict[str, NamespaceWatchQueryObjectState] = dict()

    @property
    def watched_objects(self) -> List[NamespaceWatchQueryObjectState]:
        return list(self._object_states.values())

    def emit_log(self, data):
        self.emit(self.pod_log_event_name, data)

    def process_data_state(self, data: dict, client: KubeApiRestClient):
        if not self.is_running:
            return
        uid, kind = NamespaceWatchQueryObjectState.detect(data)
        if self.collect_kube_object_state and kind in self.kinds:
            if uid not in self._object_states:
                self._object_states[uid] = NamespaceWatchQueryObjectState(
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

        if self.watch_pod_logs and kind == "pod" and uid not in self._executing_pod_loggers:
            namespace = data["metadata"]["namespace"]
            name = data["metadata"]["name"]
            pod_status = data["status"]["phase"]
            if pod_status != "Pending":
                read_logs = GetPodLogs(
                    name=name,
                    namespace=namespace,
                    since=self.pod_log_since,
                    follow=True,
                )

                def handle_error(sender, *args):
                    if len(args) == 0:
                        self.emit_error(KubeApiException("Unknown error from sender", sender))
                    else:
                        self.emit_error(args[0])

                # binding only relevant events.
                read_logs.on(read_logs.data_event_name, lambda line: self.emit_log(line))
                read_logs.on(read_logs.error_event_name, handle_error)
                self._executing_pod_loggers[uid] = read_logs
                client.query_async(read_logs)

    def stop(self, timeout: float = None, throw_error_if_not_running: bool = None):
        for q in self._executing_queries:
            q.stop(timeout=timeout, throw_error_if_not_running=throw_error_if_not_running)
        for q in self._executing_pod_loggers.values():
            q.stop(timeout=timeout, throw_error_if_not_running=throw_error_if_not_running)

        return super().stop(timeout=timeout, throw_error_if_not_running=throw_error_if_not_running)

    def log_event(self, logger: Logger, ev: Event):
        if ev.name == self.query_before_reconnect_event_name:
            if isinstance(ev.sender, GetNamespaceObjects):
                get_ns_objs: GetNamespaceObjects = ev.sender
                logger.info(
                    f"[{get_ns_objs.namespace}/{get_ns_objs.kind.plural}] "
                    + f"Watch collection for {get_ns_objs.kind.plural} lost, attempting to reconnect..."
                )
        if ev.name == self.state_changed_event_name:
            osw: NamespaceWatchQueryObjectState = ev.sender
            logger.info(f"[{osw.namespace}/{osw.kind_name.lower()}s/{osw.name}]" + f" {osw.state}")
        elif ev.name == self.pod_log_event_name:
            line: LogLine = ev.args[0]
            line.log(logger)

    def pipe_to_logger(self, logger: Logger = kube_logger, allowed_event_names=None) -> int:
        allowed_event_names = set(
            allowed_event_names
            or [
                self.state_changed_event_name,
                self.pod_log_event_name,
            ]
        )

        return super().pipe_to_logger(logger, allowed_event_names)

    def wait_for_state(
        self,
        state: KubeObjectState,
        kind: KubeObjectKind,
        name: str,
        namespace: str,
        timeout: float = None,
    ) -> KubeObjectState:
        if not isinstance(state, list):
            state = [state]

        final_state = None

        def state_reached(sender: EventHandler, event: Event):
            if event.name != self.state_changed_event_name:
                return False
            if not isinstance(event.sender, NamespaceWatchQueryObjectState):
                return False

            osw: NamespaceWatchQueryObjectState = event.sender
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
        queries: List[GetNamespaceObjects] = []
        namespaces = set(self.namespaces)
        if len(namespaces) == 0:
            namespaces = set([client.get_default_namespace()])

        for namespace in namespaces:
            for kind in list(self.kinds.values()):
                q = GetNamespaceObjects(
                    kind=kind,
                    namespace=namespace,
                    watch=self.watch,
                    field_selector=self.field_selector,
                    label_selector=self.label_selector,
                )
                q.on(q.data_event_name, lambda data: self.process_data_state(data, client))
                q.pipe(self)

                queries.append(q)
                self._executing_queries.add(q)

        client.query_async(queries)
        for q in queries:
            q.wait_until_running(timeout=None)
        self._emit_running()
        Task.wait_for_all(queries)
