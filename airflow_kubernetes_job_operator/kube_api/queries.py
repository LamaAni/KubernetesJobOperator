from logging import Logger
import logging
import datetime
import os
from sys import api_version
import kubernetes
import json
import dateutil.parser
from typing import Callable
from enum import Enum
from zthreading.events import Event

from airflow_kubernetes_job_operator.kube_api.utils import kube_logger
from yaml import emit
from airflow_kubernetes_job_operator.kube_api.client import KubeApiRestQuery, KubeApiRestClient


KUBE_API_SHOW_SERVER_LOG_TIMESTAMPS = os.environ.get("KUBE_API_SHOW_SERVER_LOG_TIMESTAMPS", "false").lower() == "true"
KUBE_API_SHOW_SERVER_DETECT_LOG_LEVEL = (
    os.environ.get("KUBE_API_SHOW_SERVER_DETECT_LOG_LEVEL", "true").lower() == "true"
)


def do_detect_log_level(line: "LogLine", msg: str):
    if "CRITICAL" in msg:
        return logging.CRITICAL
    if "ERROR" in msg:
        return logging.ERROR
    elif "WARN" in msg or "WARNING" in msg:
        return logging.WARNING
    elif "DEBUG" in msg:
        return logging.DEBUG
    else:
        return logging.INFO


KUBE_API_SHOW_SERVER_DETECT_LOG_LEVEL_METHOD = do_detect_log_level


class LogLine:
    def __init__(self, pod_name: str, namespace: str, message: str, timestamp: datetime):
        super().__init__()
        self.pod_name = pod_name
        self.namespace = namespace
        self.message = message
        self.timestamp = timestamp

    def log(self, logger: Logger = kube_logger):
        msg = self.__repr__()
        if not KUBE_API_SHOW_SERVER_DETECT_LOG_LEVEL:
            logger.info(msg)
        elif KUBE_API_SHOW_SERVER_DETECT_LOG_LEVEL_METHOD is not None:
            logger.log(
                KUBE_API_SHOW_SERVER_DETECT_LOG_LEVEL_METHOD(self, msg) or logging.INFO,
                msg,
            )
        else:
            logger.info(msg)

    def __str__(self):
        return self.message

    def __repr__(self):
        timestamp = f"[{self.timestamp}]" if KUBE_API_SHOW_SERVER_LOG_TIMESTAMPS else ""
        return timestamp + f"[{self.namespace}/pods/{self.pod_name}]: {self.message}"


class GetPodLogs(KubeApiRestQuery):
    def __init__(
        self,
        name: str,
        namespace: str = None,
        since: datetime = None,
        follow: bool = False,
        timeout: int = None,
    ):
        super().__init__(
            resource_path=None,  # Will be updated just before the run.
            method="GET",
            timeout=timeout,
        )

        self.name: str = name
        self.namespace: str = namespace
        self.since: datetime = since
        self.query_params = {
            "sinceSeconds": None if since is None else (datetime.now() - self.since),
            "follow": follow,
            "pretty": False,
            "timestamps": True,
        }

        self._active_namespace = None

    def parse_data(self, message_line: str):
        timestamp = dateutil.parser.isoparse(message_line[: message_line.index(" ")])
        message = message_line[message_line.index(" ") + 1 :]
        message = message.replace("\r", "")
        lines = []
        for message_line in message.split("\n"):
            lines.append(LogLine(self.name, self._active_namespace, message_line, timestamp))
        return lines

    def emit_data(self, data):
        for line in data:
            super().emit_data(line)

    def log_event(self, logger: Logger, ev: Event):
        if ev.name == self.data_event_name and isinstance(ev.args[0], LogLine):
            ev.args[0].log(logger)
        super().log_event(logger, ev)

    def pre_request(self, client: KubeApiRestClient):
        namespace = self.namespace or client.get_default_namespace()
        self._active_namespace = namespace
        assert namespace is not None, ValueError("Invalid namespace and could not read default namespace")
        self.resource_path = f"/api/v1/namespaces/{namespace}/pods/{self.name}/log"

        return super().pre_request(client)


class NamespaceObjectKinds(Enum):
    Pod = "pod"
    Job = "job"
    Service = "service"
    Deployment = "deployment"
    Event = "event"

    def __str__(self):
        return self.value.__str__()

    def __repr__(self):
        return self.value.__repr__()


NAMESPACE_OBJECTS_KIND_MAP = {}


def add_namespace_object_kind(name: str, compose: Callable):
    NAMESPACE_OBJECTS_KIND_MAP[str(name).lower()] = compose


add_namespace_object_kind(
    NamespaceObjectKinds.Job,
    lambda namespace: f"/apis/batch/v1/namespaces/{namespace}/jobs",
)
add_namespace_object_kind(
    NamespaceObjectKinds.Deployment,
    lambda namespace: f"/apis/apps/v1/namespaces/{namespace}/deployments",
)


class GetNamespaceObjects(KubeApiRestQuery):
    def __init__(
        self,
        kind: str,
        api_path: str = None,
        namespace: str = None,
        watch: bool = False,
        label_selector: str = None,
        field_selector: str = None,
    ):
        super().__init__(
            resource_path=None,
            method="GET",
            query_params={
                "pretty": False,
                "fieldSelector": field_selector or "",
                "labelSelector": label_selector or "",
                "watch": watch,
            },
        )
        self.api_version = api_version
        self.kind = kind
        self.namespace = namespace

    @classmethod
    def to_resource_path(cls, kind: str, namespace):
        # FIXME: Change uri (example: /api/v1/namespaces) to a repo values.
        kind = str(kind).lower()
        if kind in NAMESPACE_OBJECTS_KIND_MAP:
            return NAMESPACE_OBJECTS_KIND_MAP[kind](namespace)
        else:
            return f"/api/v1/namespaces/{namespace}/{kind + 's'}"

    def parse_data(self, line):
        rsp = json.loads(line)
        return rsp

    def emit_data(self, data: dict):
        if "kind" in data:
            data_kind: str = data["kind"]
            if data_kind.endswith("List"):
                item_kind = data_kind[:-4]
                for item in data["items"]:
                    item["kind"] = item_kind
                    super().emit_data(item)
            else:
                super().emit_data(data)
        elif "type" in data:
            # as event.
            event_object = data["object"]
            event_object["event_type"] = data["type"]
            super().emit_data(event_object)

    def pre_request(self, client: KubeApiRestClient):
        self.resource_path = self.to_resource_path(self.kind, self.namespace or client.get_default_namespace())
        return super().pre_request(client)
