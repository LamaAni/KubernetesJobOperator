import datetime
import kubernetes
import json
import dateutil.parser
from typing import List
from enum import Enum
from airflow_kubernetes_job_operator.kube_api.client import KubeApiRestQuery, KubeApiRestClient
from airflow_kubernetes_job_operator.kube_api.queries import GetNamespaceObjects, GetPodLogs, NamespaceObjectKinds


class NamespaceWatchQuery(KubeApiRestQuery):
    def __init__(
        self,
        kinds: List[str] = [NamespaceObjectKinds.Pod, NamespaceObjectKinds.Job, NamespaceObjectKinds.Service],
        namespace: str = None,
        watch: bool = True,
        label_selector: str = None,
        field_selector: str = None,
        watch_pod_logs: bool = True,
        pod_log_event_name: str = "log",
    ):
        super().__init__(
            None,
            method="GET",
        )

        self.watch = watch
        self.namespace = namespace
        self.label_selector = label_selector
        self.field_selector = field_selector
        self.watch_pod_logs = watch_pod_logs
        self.pod_log_event_name = pod_log_event_name

        self.kinds = kinds

    def _query_loop(self, client: KubeApiRestClient):
        # specialized loop. Uses the event handler to read multiple sourced events,
        # and waits for the stream to stop.

        queries = []

        for kind in self.kinds:
            queries.append(
                GetNamespaceObjects(
                    kind=kind,
                    namespace=self.namespace,
                    watch=self.watch,
                    field_selector=self.field_selector,
                    label_selector=self.label_selector,
                )
            )

        for rsp in client.stream(queries):
            data = self.parse_data(rsp)
            self.emit_data(data)

        self.emit(self.complete_event_name)
