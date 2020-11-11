from logging import Logger
import logging
from datetime import datetime
import json
from typing import Union, List, Callable
import dateutil.parser
from zthreading.events import Event

from airflow_kubernetes_job_operator.kube_api.exceptions import KubeApiClientException, KubeApiException
from airflow_kubernetes_job_operator.kube_api.utils import kube_logger, not_empty_string
from airflow_kubernetes_job_operator.kube_api.collections import (
    KubeResourceKind,
    KubeResourceState,
    KubeResourceDescriptor,
)
from airflow_kubernetes_job_operator.kube_api.client import KubeApiRestQuery, KubeApiRestClient


def default_detect_log_level(line: "LogLine", msg: str):
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


class LogLine:
    show_kubernetes_log_timestamps: bool = False
    autodetect_kuberentes_log_level: bool = True
    detect_kubernetes_log_level: Callable = None

    def __init__(self, pod_name: str, namespace: str, message: str, timestamp: datetime = None):
        """GetPodLogs log line generated info object.

        Args:
            pod_name (str): The name of the pod
            namespace (str): The namespace of the pod
            message (str): The message
            timestamp (datetime): The log timestamp.
        """
        super().__init__()
        self.pod_name = pod_name
        self.namespace = namespace
        self.message = message
        self.timestamp = timestamp or datetime.now()

    def log(self, logger: Logger = kube_logger):
        msg = self.__repr__()
        auto_detect_method = self.detect_kubernetes_log_level or default_detect_log_level
        if not self.autodetect_kuberentes_log_level or not isinstance(auto_detect_method, Callable):
            logger.info(msg)
        else:
            logger.log(
                auto_detect_method(self, msg) or logging.INFO,
                msg,
            )

    def __str__(self):
        return self.message

    def __repr__(self):
        timestamp = f"[{self.timestamp}]" if self.show_kubernetes_log_timestamps else ""
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
        """Returns the pod logs for a pod. Can follow the pod logs
        in real time.

        Args:
            name (str): The name of the pod.
            namespace (str, optional): The pod namespace. Defaults to None.
            since (datetime, optional): Since when to start the log read. If None -> all logs.
                Defaults to None.
            follow (bool, optional): If true, keep streaming pod logs. Defaults to False.
            timeout (int, optional): The read timeout, if specified will error if logs were not
                returned in time. Defaults to None.
        """
        assert not_empty_string(name), ValueError("name must be a non empty string")
        assert not_empty_string(namespace), ValueError("namespace must be a non empty string")

        kind: KubeResourceKind = KubeResourceKind.get_kind("Pod")
        super().__init__(
            resource_path=kind.compose_resource_path(namespace=namespace, name=name, suffix="log"),
            method="GET",
            timeout=timeout,
            auto_reconnect=follow,
        )

        self.kind = kind
        self.name: str = name
        self.namespace: str = namespace
        self.since: datetime = since
        self.query_params = {
            "follow": follow,
            "pretty": False,
            "timestamps": True,
        }

        self.since = since
        self._last_timestamp = None
        self._active_namespace = None

    def pre_request(self, client: "KubeApiRestClient"):
        super().pre_request(client)

        # Updating the since argument.
        last_ts = self.since if self.since is not None and self.since > self._last_timestamp else self._last_timestamp

        since_seconds = None
        if last_ts is not None:
            since_seconds = (datetime.now() - last_ts).total_seconds()
            if since_seconds < 0:
                since_seconds = 0

        self.query_params["sinceSeconds"] = since_seconds

    def on_reconnect(self, client: KubeApiRestClient):
        # updating the since property.
        if not self.query_running or not self.auto_reconnect:
            # if the query is not running then we have reached the pods log end.
            # we should disconnect, otherwise we should have had an error.
            self.auto_reconnect = False
            return False

        try:
            pod = client.query(
                GetNamespaceResources(
                    kind=self.kind,
                    namespace=self.namespace,
                    name=self.name,
                )
            )
            self.auto_reconnect = pod is not None and KubeResourceDescriptor(pod).state == KubeResourceState.Running
            return self.auto_reconnect
        except Exception as ex:
            self.auto_reconnect = False
            raise ex

    def parse_data(self, message_line: str):
        self._last_timestamp = datetime.now()
        timestamp = dateutil.parser.isoparse(message_line[: message_line.index(" ")])

        message = message_line[message_line.index(" ") + 1 :]  # noqa: E203
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


class GetNamespaceResources(KubeApiRestQuery):
    def __init__(
        self,
        kind: Union[str, KubeResourceState],  # type:ignore
        namespace: str,
        name: str = None,
        api_version: str = None,
        watch: bool = False,
        label_selector: str = None,
        field_selector: str = None,
    ):
        """Returns a collection of api resources. Can watch for changes in a namespace.

        Args:
            kind (Union[str, KubeResourceState]): The resource kind to look for.
            name (str, optional): The resource name to look for. If none then all resources. Defaults to None.
            api_version (str, optional): The resource api_version. Defaults to None.
            watch (bool, optional): If true, continue watching for changes. Defaults to False.
            label_selector (str, optional): A kubernetes label selector to filter resources. Defaults to None.
            field_selector (str, optional): A kubernetes field selector to filter resources. Defaults to None.
        """
        kind: KubeResourceKind = (
            kind if isinstance(kind, KubeResourceKind) else KubeResourceKind.get_kind(kind)  # type:ignore
        )
        super().__init__(
            resource_path=kind.compose_resource_path(namespace=namespace, name=name, api_version=api_version),
            method="GET",
            query_params={
                "pretty": False,
                "fieldSelector": field_selector or "",
                "labelSelector": label_selector or "",
                "watch": watch,
            },
            auto_reconnect=watch,
            throw_on_if_first_api_call_fails=name is None,
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
        except KubeApiClientException as ex:
            if ex.rest_api_exception.status == 404:
                raise KubeApiException(
                    f"Resource {self.resource_path} of kind '{self.kind.api_version}/{self.kind.name}' not found"
                )
        except Exception as ex:
            raise ex


class GetAPIResources(KubeApiRestQuery):
    def __init__(
        self,
        api="apps/v1",
    ):
        """Returns a dictionary of api resources

        Args:
            api (str, optional): The api name. Defaults to "apps/v1".
        """
        super().__init__(
            f"/apis/{api}",
        )

    def parse_data(self, data):
        return json.loads(data)


class GetAPIVersions(KubeApiRestQuery):
    def __init__(
        self,
    ):
        """Returns a dictionary of api versions."""
        super().__init__(
            "/apis",
            method="GET",
        )

    def parse_data(self, data):
        """ Override data parse """
        rslt = json.loads(data)
        prased = {}
        for grp in rslt.get("groups", []):
            group_name = grp.get("name")
            preferred = grp.get("preferredVersion", {}).get("version")
            for ver_info in grp.get("versions", []):
                group_version = ver_info.get("groupVersion")
                version = ver_info.get("version")
                assert group_version is not None, KubeApiException("Invalid group version returned from the api")
                prased[group_version] = {
                    "group_name": group_name,
                    "is_preferred": preferred == version,
                    "version": version,
                    "group": grp,
                }
        return prased

    @classmethod
    def get_existing_api_kinds(cls, client: KubeApiRestClient, all_kinds: List[KubeResourceKind] = None):
        """Filter the list of kinds an returns only the api kinds
        found on the server.

        Args:
            all_kinds (List[KubeResourceKind], optional): The resource kinds to check. If none
            checks all available kinds. Defaults to None.

        Returns:
            List[KubeResourceKind]: The list of kinds that exist on the server.
        """
        all_kinds = all_kinds or KubeResourceKind.all()
        apis = client.query(GetAPIVersions())
        return [k for k in all_kinds if k.api_version == "v1" or k.api_version in apis]
