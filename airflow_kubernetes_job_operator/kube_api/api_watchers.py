import json
import re
import kubernetes

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


class KubeApiWatchStream(Task):
    """Kubernetes api watcher"""

    def __init__(
        self,
        response_factory: Callable,
        reconnect_max_retries: int = 20,
        reconnect_wait_timeout: int = 5,
        response_wait_timeout: int = 5,
        api_event_name: str = "api_event",
        since: datetime = None,
    ):
        super().__init__(self._watch_loop, use_async_loop=False, use_daemon_thread=True, event_name="watch_ended")

        self._get_response = response_factory
        self.reconnect_wait_timeout = reconnect_wait_timeout
        self.reconnect_max_retries = reconnect_max_retries
        self.response_wait_timeout = response_wait_timeout
        self.api_event_name = api_event_name
        self._since = since

        # internal
        self._close_response_name_event_name = f"close_response_streams_for_watcher ({id(self)})"

    def read_event(self, data):
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
    def read_events_after(self):
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
            sleep(self.reconnect_wait_timeout)

        # adding required streaming query values.
        kwargs["_preload_content"] = False
        kwargs["_request_timeout"] = self.response_wait_timeout

        def close_response():
            if response is not None:
                response.close()

        close_response_stream_event_handle = self.on(self._close_response_name_event_name, close_response)

        # always read until stopped or resource dose not exist.
        try:
            while True:
                try:
                    if self._since is not None:
                        offset_read_seconds = (datetime.now() - self._last_read_time).seconds
                        kwargs["since_seconds"] = str(offset_read_seconds)

                    # closing any current response streams.
                    close_response()

                    response = self._get_response(*args, **kwargs)
                    was_started = True

                    for line in _kube_api_lines_reader(response):
                        # Is actually connected, therefore reading the response stream.
                        reconnect_attempts = 0
                        self._since = datetime.now()
                        self.emit(self.api_event_name, self.read_event(line))

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

    def crate_watch_url(self, namespace):
        # FIXME: Change uri (example: /api/v1/namespaces) to a repo values.
        if self == KubeApiNamespaceWatcherKind.Pod:
            return f"/api/v1/namespaces/{namespace}/pods"
        elif self == KubeApiNamespaceWatcherKind.Job:
            return f"/apis/batch/v1/namespaces/{namespace}/jobs"
        elif self == KubeApiNamespaceWatcherKind.Service:
            return f"/api/v1/namespaces/{namespace}/services"
        elif self == KubeApiNamespaceWatcherKind.Deployment:
            return f"/apis/apps/v1beta2/namespaces/{namespace}/deployments"
        elif self == KubeApiNamespaceWatcherKind.Event:
            return f"/api/v1/namespaces/{namespace}/events"
        raise Exception("Watch type not found: " + str(self))


class KubeApiNamespaceWatcher(KubeApiWatchStream):
    def __init__(
        self,
        namespace: str,
        kind: KubeApiNamespaceWatcherKind = KubeApiNamespaceWatcherKind.Event,
        field_selector: str = None,
        label_selector: str = None,
        client: kubernetes.client.CoreV1Api() = None,
        reconnect_max_retries=20,
        reconnect_wait_timeout=5,
        response_wait_timeout=5,
        api_event_name="api_event",
        since=None,
    ):
        self.kind = kind
        self.namespace = namespace
        self.field_selector = field_selector
        self.label_selector = label_selector
        self.client = client or kubernetes.client.CoreV1Api()

        super().__init__(
            response_factory=self._create_watch_response_stream_factory,
            reconnect_max_retries=reconnect_max_retries,
            reconnect_wait_timeout=reconnect_wait_timeout,
            response_wait_timeout=response_wait_timeout,
            api_event_name=api_event_name,
            since=since,
        )

    def _create_watch_response_stream_factory(self, *args, **kwargs):
        path_params = {"namespace": self.namespace}
        query_params = {
            "pretty": False,
            "_continue": True,
            "fieldSelector": self.field_selector or "",
            "labelSelector": self.label_selector or "",
            "watch": True,
        }

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
            self.kind.crate_watch_url(self.namespace),
            "GET",
            path_params,
            query_params,
            header_params,
            body=body_params,
            post_params=form_params,
            files=local_var_files,
            response_type="V1EventList",
            auth_settings=auth_settings,
            async_req=False,
            _return_http_data_only=False,
            _preload_content=False,
            collection_formats=collection_formats,
        )

        return api_call[0]
