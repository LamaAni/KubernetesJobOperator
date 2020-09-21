from logging import Logger
import logging
import datetime
import os
import json
import dateutil.parser
from zthreading.events import Event

from airflow_kubernetes_job_operator.kube_api.utils import kube_logger, not_empty_string
from airflow_kubernetes_job_operator.kube_api.collections import KubeObjectKind
from airflow_kubernetes_job_operator.kube_api.client import KubeApiRestQuery


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
        assert not_empty_string(name), ValueError("name must be a non empty string")
        assert not_empty_string(namespace), ValueError("namespace must be a non empty string")

        kind: KubeObjectKind = KubeObjectKind.get_kind("Pod")
        super().__init__(
            resource_path=kind.compose_resource_path(namespace=namespace, name=name, suffix="log"),
            method="GET",
            timeout=timeout,
        )

        self.kind = kind
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
            lines.append(LogLine(self.name, self.namespace, message_line, timestamp))
        return lines

    def emit_data(self, data):
        for line in data:
            super().emit_data(line)

    def log_event(self, logger: Logger, ev: Event):
        if ev.name == self.data_event_name and isinstance(ev.args[0], LogLine):
            ev.args[0].log(logger)
        super().log_event(logger, ev)


class GetNamespaceObjects(KubeApiRestQuery):
    def __init__(
        self,
        kind: str,
        namespace: str,
        api_version: str = None,
        watch: bool = False,
        label_selector: str = None,
        field_selector: str = None,
    ):
        kind: KubeObjectKind = kind if isinstance(kind, KubeObjectKind) else KubeObjectKind.get_kind(kind)
        super().__init__(
            resource_path=kind.compose_resource_path(namespace, api_version=api_version),
            method="GET",
            query_params={
                "pretty": False,
                "fieldSelector": field_selector or "",
                "labelSelector": label_selector or "",
                "watch": watch,
            },
        )
        self.kind = kind
        self.namespace = namespace

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

    def query_loop(self, client):
        try:
            return super().query_loop(client)
        except Exception as ex:
            raise ex


class GetAPIResources(KubeApiRestQuery):
    def __init__(
        self,
        api="apps/v1",
    ):
        super().__init__(
            f"/apis/{api}",
        )

    def parse_data(self, data):
        return json.loads(data)


class GetAPIVersions(KubeApiRestQuery):
    def __init__(
        self,
    ):
        super().__init__(
            "/apis",
            method="GET",
        )

    def parse_data(self, data):
        return json.loads(data)