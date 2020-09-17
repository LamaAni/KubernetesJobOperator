import datetime
from datetime import time
from logging import Logger
import queue
import kubernetes
import json
import dateutil.parser

from zthreading.events import EventHandler, Event
from weakref import WeakSet, WeakValueDictionary
from typing import List, Dict, Callable, Generator
from enum import Enum

from zthreading.tasks import Task
from airflow_kubernetes_job_operator.kube_api.utils import kube_logger
from airflow_kubernetes_job_operator.kube_api.client import KubeApiRestQuery, KubeApiRestClient
from airflow_kubernetes_job_operator.kube_api.queries import (
    GetNamespaceObjects,
    GetPodLogs,
    NamespaceObjectKinds,
    LogLine,
)


class KubeObjectState(Enum):
    Pending = "Pending"
    Active = "Active"
    Succeeded = "Succeeded"
    Failed = "Failed"
    Running = "Running"
    Deleted = "Deleted"

    def __str__(self) -> str:
        return self.value

    @classmethod
    def get_state(cls, yaml: dict):
        kind = yaml.get("kind")
        status = yaml.get("status", {})
        spec = yaml.get("spec", {})

        if kind == "Job":
            job_status = "Pending"
            back_off_limit = int(spec["backoffLimit"])
            if "startTime" in status:
                if "completionTime" in status:
                    job_status = KubeObjectState.Succeeded
                elif "failed" in status and int(status["failed"]) > back_off_limit:
                    job_status = KubeObjectState.Failed
                else:
                    job_status = KubeObjectState.Running
            return job_status

        elif kind == "Pod":
            pod_phase = status["phase"]
            container_status = status.get("containerStatuses", [])
            for container_status in container_status:
                if "state" in container_status:
                    if (
                        "waiting" in container_status["state"]
                        and "reason" in container_status["state"]["waiting"]
                        and "BackOff" in container_status["state"]["waiting"]["reason"]
                    ):
                        return KubeObjectState.Failed
                    if "error" in container_status["state"]:
                        return KubeObjectState.Failed

            if pod_phase == "Pending":
                return KubeObjectState.Pending
            elif pod_phase == "Running":
                return KubeObjectState.Running
            elif pod_phase == "Completed":
                return KubeObjectState.Succeeded
            elif pod_phase == "Failed":
                return KubeObjectState.Failed
            return pod_phase
        else:
            return KubeObjectState.Active


class NamespaceWatchQueryObjectState(EventHandler):
    def __init__(
        self,
        uid: str,
        status_changed_event_name="status_changed",
        state_changed_event_name="state_changed",
        deleted_event_name="deleted",
    ) -> None:
        super().__init__()
        self.yaml = {"metadata": {"uid": uid}}
        self._deleted = False
        self.status_changed_event_name = status_changed_event_name
        self.deleted_event_name = deleted_event_name
        self.state_changed_event_name = state_changed_event_name
        self._last_state: KubeObjectState = None
        self._state: KubeObjectState = None

    @classmethod
    def detect(cls, yaml: dict):
        uid = yaml.get("metadata", {}).get("uid", None)
        kind = yaml.get("kind", "[unknown kube element]").lower()
        return uid, kind

    @property
    def uid(self) -> str:
        return self.metadata["uid"]

    @property
    def kind(self) -> str:
        return self.yaml.get("kind", "{undefined}")

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
        return self.yaml.get("metadata", {})

    @property
    def status(self) -> dict:
        return self.yaml.get("status", {})

    @property
    def state(self) -> KubeObjectState:
        if self._state is None:
            if self.deleted:
                self._state = KubeObjectState.Deleted
            else:
                self._state = KubeObjectState.get_state(self.yaml)
        return self._state

    def update(self, object_yaml):
        if object_yaml.get("event_type", None) == "DELETED":
            self._deleted = True
            self.emit(self.deleted_event_name, self)

        has_status_changed = json.dumps(self.status) != json.dumps(object_yaml.get("status", {}))
        if has_status_changed:
            self.emit(self.status_changed_event_name, self)

        self.yaml.update(object_yaml)

        # reset the state.
        self._state = None

        if self._last_state != self.state:
            self.emit(self.state_changed_event_name, self)
        self._last_state = self.state


class NamespaceWatchQuery(KubeApiRestQuery):
    status_changed_event_name = "status_changed"
    state_changed_event_name = "state_changed"
    deleted_event_name = "deleted"

    def __init__(
        self,
        kinds: List[str] = [
            NamespaceObjectKinds.Pod,
            NamespaceObjectKinds.Job,
            NamespaceObjectKinds.Service,
            NamespaceObjectKinds.Deployment,
        ],
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

        self.watch = watch
        self.namespace = namespace
        self.label_selector = label_selector
        self.field_selector = field_selector
        self.watch_pod_logs = watch_pod_logs
        self.pod_log_event_name = pod_log_event_name
        self.pod_log_since = pod_log_since
        self.collect_kube_object_state = collect_kube_object_state

        self.kinds = [str(k).lower() for k in kinds]

        self._executing_queries: List[KubeApiRestQuery] = WeakSet()
        self._executing_pod_loggers: Dict[str, GetPodLogs] = WeakValueDictionary()
        self._object_states: Dict[str, NamespaceWatchQueryObjectState] = dict()

    def emit_log(self, data):
        self.emit(self.pod_log_event_name, data)

    def process_data_state(self, data: dict, client: KubeApiRestClient):
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
                read_logs = GetPodLogs(name, namespace, self.pod_log_since, True)
                # binding only relevant events.
                read_logs.on(read_logs.data_event_name, lambda line: self.emit_log(line))
                read_logs.on(read_logs.error_event_name, lambda sender, err: self.emit(self.error_event_name, err))
                self._executing_pod_loggers[uid] = read_logs
                client.async_query(read_logs)

    def stop(self, timeout: float = None, throw_error_if_not_running: bool = None):
        for q in self._executing_queries:
            q.stop(timeout=timeout, throw_error_if_not_running=throw_error_if_not_running)
        for q in self._executing_pod_loggers.values():
            q.stop(timeout=timeout, throw_error_if_not_running=throw_error_if_not_running)

        return super().stop(timeout=timeout, throw_error_if_not_running=throw_error_if_not_running)

    def log_event(self, logger: Logger, ev: Event):
        if ev.name == self.state_changed_event_name:
            state: NamespaceWatchQueryObjectState = ev.args[0]
            logger.info(f"[{state.namespace}/{state.kind.lower()}s/{state.name}] {state.state}")
        elif ev.name == self.pod_log_event_name:
            line: LogLine = ev.args[0]
            line.log(logger)

    def bind_logger(self, logger: Logger = kube_logger, allowed_event_names=None) -> int:
        allowed_event_names = set(
            allowed_event_names
            or [
                self.state_changed_event_name,
                self.pod_log_event_name,
            ]
        )

        return super().bind_logger(logger, allowed_event_names)

    def query_loop(self, client: KubeApiRestClient):
        # specialized loop. Uses the event handler to read multiple sourced events,
        # and waits for the stream to stop.
        queries: List[GetNamespaceObjects] = []

        for kind in self.kinds:
            q = GetNamespaceObjects(
                kind=kind,
                namespace=self.namespace,
                watch=self.watch,
                field_selector=self.field_selector,
                label_selector=self.label_selector,
            )
            q.on(q.data_event_name, lambda data: self.process_data_state(data, client))
            q.pipe(self)

            queries.append(q)
            self._executing_queries.add(q)

        client.async_query(queries)
        Task.wait_for_all(queries)