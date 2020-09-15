import json
import re
import kubernetes
import dateutil.parser

from enum import Enum

from typing import Callable
from datetime import datetime
from time import sleep
from zthreading.tasks import Task
from urllib3.connectionpool import MaxRetryError, ReadTimeoutError, TimeoutError
from urllib3.response import HTTPResponse

from airflow_kubernetes_job_operator.event_handler import EventHandler
from kubernetes.stream.ws_client import ApiException

from airflow_kubernetes_job_operator.kube_api.exceptions import KubeApiWatcherException, KubeApiWatcherParseException

import threading


def _kube_api_lines_reader(response: HTTPResponse):
    """INTERNAL. Helper yield method. Parses the streaming http response
    to lines (can by async!)

    Yields:
        str: The line
    """
    prev = ""
    for chunk in response.stream(decode_content=False):
        if isinstance(chunk, bytes):
            chunk = chunk.decode("utf8")
        chunk = prev + chunk
        lines = chunk.split("\n")
        if not chunk.endswith("\n"):
            prev = lines[-1]
            lines = lines[:-1]
        else:
            prev = ""
        for line in lines:
            if line:
                yield line


def _clean_dictionary_nulls(d: dict):
    for k in list(d.keys()):
        if d[k] is None:
            del d[k]

    return d


class KubeApiObject(dict):
    def __init__(self, iterable, event_type=None):
        super().__init__(iterable)
        self.event_type = event_type


class KubeApiWatchStream(Task):
    """Kubernetes api watcher"""

    def __init__(
        self,
        response_factory: Callable,
        reconnect_max_retries: int = 20,
        reconnect_wait_timeout: int = 5,
        api_event_name: str = "api_event",
        since: datetime = None,
    ):
        super().__init__(self._watch_loop, use_async_loop=False, use_daemon_thread=True, event_name="watch_ended")

        self._get_response = response_factory
        self.reconnect_wait_timeout = reconnect_wait_timeout
        self.reconnect_max_retries = reconnect_max_retries
        self.api_event_name = api_event_name
        self._since = since

        # internal
        self._close_response_name_event_name = f"close_response_streams_for_watcher ({id(self)})"

    def parse_data(self, data):
        """Parses the event data.

        Arguments:

            data {str} -- The event data.

        Returns:

            dict/str -- Returns the parsed data, depending on read_as_dict.
        """
        try:
            return json.loads(data)
        except Exception as ex:
            raise KubeApiWatcherException("Failed to parse kubernetes api event", ex)

    @property
    def parse_datas_after(self):
        return self._since

    def _watch_loop(self, *args, **kwargs):
        """FOR INTERNAL USE ONLY! Do not call this function outside the scope of this object.
        The main reader loop that emits data events form the kubernetes watch api.

        Emits:
            api_event
        """

        was_started = False
        reconnect_attempts = 0
        response: HTTPResponse = None

        def can_attempt_reconnect(e: Exception):
            nonlocal reconnect_attempts
            reconnect_attempts += 1
            if reconnect_attempts == self.reconnect_max_retries:
                raise e
            self.emit("warning", self, e)
            self.emit("reconnect")
            sleep(self.reconnect_wait_timeout)

        def close_response():
            if response is not None:
                response.close()

        def advance_since():
            self._since = datetime.now()

        close_response_stream_event_handle = self.on(self._close_response_name_event_name, close_response)

        # always read until stopped or resource dose not exist.
        try:
            while True:
                try:
                    if self._since is not None:
                        offset_read_seconds = (datetime.now() - self._since).seconds
                        kwargs["since_seconds"] = str(offset_read_seconds)

                    # closing any current response streams.
                    close_response()

                    response = self._get_response(*args, **kwargs)
                    was_started = True
                    advance_since()

                    for line in _kube_api_lines_reader(response):
                        advance_since()
                        # Is actually connected, therefore reading the response stream.
                        reconnect_attempts = 0
                        data = self.parse_data(line)

                        self.emit(self.api_event_name, data)

                    # clean exit.
                    close_response()

                except ApiException as e:
                    # exception_body = json.loads(e.body)
                    if e.reason == "Not Found" or e.reason == "Bad Request":
                        if was_started:
                            self.emit("warning", self, e)
                            break
                        else:
                            raise e
                except MaxRetryError as e:
                    can_attempt_reconnect(e)
                except ReadTimeoutError as e:
                    can_attempt_reconnect(e)
                except TimeoutError as e:
                    can_attempt_reconnect(e)
                except Exception as e:
                    raise e

        except Exception as e:
            self.emit("error", self, KubeApiWatcherException(e))
        finally:
            # clearing the event.
            self.clear(self._close_response_name_event_name, close_response_stream_event_handle)
            self.stop_all_streams()
            close_response()

    def stream(self, event_name=None, timeout=None, use_async_loop=False, process_event_data=lambda ev: ev.args[0]):
        """Creates an event stream that will collect any event from this handler.

        Args:
            event_name (str, optional): The event name to stream. Defaults to None. If none, streams
                all events
            timeout (float, optional): Timeout between events. Defaults to None.
            use_async_loop (bool, optional): Use an asyncio loop to perform the stream. This would result
                in an asyncio compatible stream. Defaults to False.
            process_event_data (Callable, optional): A method to be called on any event value. The
                result of this method is passed to the stream. Defaults to None.

        Yields:
            Generator|AsyncGenerator of Event | the result of process_event_data, defaults to a json object of the parsed
            kube api event.
        """
        event_name = event_name or self.api_event_name
        return super().stream(
            event_name=event_name, timeout=timeout, use_async_loop=use_async_loop, process_event_data=process_event_data
        )

    def stop(self, timeout=None, throw_error_if_not_running=False):
        """Stop the executing watcher.

        Args:
            timeout (float, optional): If exists, the stop process will try and wait for the
                task to complete for {timeout} before forcefully stopping. Defaults to None.
        """
        # Emit the event to close all active response streams. This would allow the stream to attempt and
        # close natively.
        self.emit(self._close_response_name_event_name)

        # aborting any executing streams. We are at stop.
        self.stop_all_streams()

        # Call the thread stop event.
        return super().stop(timeout=timeout, throw_error_if_not_running=throw_error_if_not_running)


class KubeApiNamespaceWatcherKind(Enum):
    Pod = "Pod"
    Job = "Job"
    Service = "Service"
    Deployment = "Deployment"
    Event = "Event"


class KubeApiNamespaceWatcher(KubeApiWatchStream):
    def __init__(
        self,
        namespace: str,
        kind: str = KubeApiNamespaceWatcherKind.Event,
        field_selector: str = None,
        label_selector: str = None,
        client: kubernetes.client.CoreV1Api() = None,
        reconnect_max_retries=20,
        reconnect_wait_timeout=5,
        response_wait_timeout=None,
        api_event_name="api_event",
        since=None,
    ):
        self.kind = kind
        self.namespace = namespace
        self.field_selector = field_selector
        self.label_selector = label_selector
        self.response_wait_timeout = response_wait_timeout
        self.client: kubernetes.client.CoreV1Api = client or kubernetes.client.CoreV1Api()

        super().__init__(
            response_factory=self._create_watch_response_stream_factory,
            reconnect_max_retries=reconnect_max_retries,
            reconnect_wait_timeout=reconnect_wait_timeout,
            api_event_name=api_event_name,
            since=since,
        )

    def parse_data(self, data):
        return super().parse_data(data)

    @classmethod
    def crate_watch_url(cls, kind, namespace):
        # FIXME: Change uri (example: /api/v1/namespaces) to a repo values.
        if kind == KubeApiNamespaceWatcherKind.Pod:
            return f"/api/v1/namespaces/{namespace}/pods"
        elif kind == KubeApiNamespaceWatcherKind.Job:
            return f"/apis/batch/v1/namespaces/{namespace}/jobs"
        elif kind == KubeApiNamespaceWatcherKind.Service:
            return f"/api/v1/namespaces/{namespace}/services"
        elif kind == KubeApiNamespaceWatcherKind.Deployment:
            return f"/apis/apps/v1beta2/namespaces/{namespace}/deployments"
        elif kind == KubeApiNamespaceWatcherKind.Event:
            return f"/api/v1/namespaces/{namespace}/events"
        raise Exception("Watch type not found: " + str(kind))

    def _create_watch_response_stream_factory(self, *args, **kwargs):
        path_params = {"namespace": self.namespace}
        query_params = {
            "pretty": False,
            "_continue": True,
            "fieldSelector": self.field_selector,
            "labelSelector": self.label_selector,
            "watch": True,
        }

        query_params.update(kwargs)
        _clean_dictionary_nulls(query_params)

        # watch request, default values.
        body_params = None
        local_var_files = {}
        form_params = []
        collection_formats = {}

        # Set header
        header_params = {}
        header_params["Accept"] = self.client.api_client.select_header_accept(
            [
                "application/json",
                "application/yaml",
                "application/vnd.kubernetes.protobuf",
                "application/json;stream=watch",
                "application/vnd.kubernetes.protobuf;stream=watch",
            ]
        )
        header_params["Content-Type"] = self.client.api_client.select_header_content_type(["*/*"])

        # Authentication
        auth_settings = ["BearerToken"]

        api_call = self.client.api_client.call_api(
            self.crate_watch_url(self.kind, self.namespace),
            "GET",
            path_params,
            query_params,
            header_params,
            body=body_params,
            post_params=form_params,
            files=local_var_files,
            response_type="V1EventList",
            auth_settings=auth_settings,
            _return_http_data_only=False,
            _preload_content=False,
            _request_timeout=self.response_wait_timeout,
            collection_formats=collection_formats,
            async_req=True,
        ).get()

        return api_call[0]


class KubeApiPodLogWatcherLine:
    def __init__(self, message: str, timestamp: datetime, namespace: str = None, name: str = None):
        super().__init__()

        self.message = message
        self.timestamp = timestamp
        self.name = name
        self.namespace = namespace

    def get_log_line(self):
        return f"[{self.timestamp}][{self.namespace}.{self.name}]: {self.message}"

    def __str__(self):
        return self.message

    def __repr__(self):
        return self.get_log_line()


class KubeApiPodLogWatcher(KubeApiWatchStream):
    def __init__(
        self,
        name: str,
        namespace: str,
        container: str = None,
        previous: bool = False,
        tail_lines: int = None,
        client: kubernetes.client.CoreV1Api() = None,
        read_current_logs_on_first_start: bool = True,
        reconnect_max_retries=20,
        reconnect_wait_timeout=5,
        api_event_name="log",
        since=None,
    ):
        self.name = name
        self.namespace = namespace
        self.client: kubernetes.client.CoreV1Api = client or kubernetes.client.CoreV1Api()
        self.container = container
        self.previous = previous
        self.tail_lines = tail_lines
        self.read_timestamps = True
        self._do_read_current_logs = read_current_logs_on_first_start
        super().__init__(
            response_factory=self._create_pod_log_response,
            reconnect_max_retries=reconnect_max_retries,
            reconnect_wait_timeout=reconnect_wait_timeout,
            api_event_name=api_event_name,
            since=since,
        )

    def _watch_loop(self, *args, **kwargs):
        if self._do_read_current_logs:
            self.read_currnet_logs(emit_events=True, collect_events=False)
            self._do_read_current_logs = False
        return super()._watch_loop(*args, **kwargs)

    def parse_data(self, data: str):
        ts = None
        if self.read_timestamps:
            ts = data[0 : data.index(" ")]
            data = data[data.index(" ") + 1 :]
            try:
                ts = dateutil.parser.isoparse(ts)
            except Exception:
                pass
        return KubeApiPodLogWatcherLine(data, ts, name=self.name, namespace=self.namespace)

    def _create_pod_log_command_args(self):
        command_args = {
            "previous": self.previous,
            "tail_lines": self.tail_lines,
            "container": self.container,
            "timestamps": self.read_timestamps,
        }

        return _clean_dictionary_nulls(command_args)

    def _create_pod_log_response(self):
        rsp = self.client.read_namespaced_pod_log_with_http_info(
            name=self.name,
            namespace=self.namespace,
            follow=True,
            **self._create_pod_log_command_args(),
            _preload_content=False,
        )

        return rsp[0]

    def read_currnet_logs(self, emit_events=False, collect_events=True):
        """Read all of the current logs from the resource. Helper method."""
        log_chunks = self.client.read_namespaced_pod_log(
            self.name, self.namespace, **self._create_pod_log_command_args(), async_req=True
        ).get()
        if not isinstance(log_chunks, list):
            log_chunks = log_chunks.strip().split("\n")

        log_lines = []
        for chunk in log_chunks:
            for line in chunk.split("\n"):
                data = self.parse_data(line)
                if collect_events:
                    log_lines.append(data)
                self.emit(self.api_event_name, data)

        return log_lines if collect_events else None
