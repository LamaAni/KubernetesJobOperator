import json
import re

from typing import Callable
from datetime import datetime
from time import sleep
from zthreading.tasks import Task
from urllib3.connectionpool import MaxRetryError, ReadTimeoutError, TimeoutError
from urllib3.response import HTTPResponse

from airflow_kubernetes_job_operator.event_handler import EventHandler
from kubernetes.stream.ws_client import ApiException

from airflow_kubernetes_job_operator.watchers.exceptions import KubeApiWatcherException, KubeApiWatcherParseException

import threading


def _kube_api_lines_reader(response: HTTPResponse):
    """INTERNAL. Helper yield method. Parses the streaming http response
    to lines (can by async!)

    Yields:
        str: The line
    """
    prev = ""
    for chunk in response.read_chunked(decode_content=False):
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


class KubeApiWatcher(Task):
    def __init__(
        self,
        create_response_stream: Callable,
        reconnect_max_retries: int = 20,
        reconnect_wait_timeout: int = 5,
        response_wait_timeout: int = 5,
        api_event_name: str = "api_event",
        since: datetime = None,
    ):
        super().__init__(self._watch_loop, use_async_loop=False, use_daemon_thread=True, event_name="watch_ended")

        self._create_response_stream = create_response_stream
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
            if self.read_as_dict:
                return json.loads(data)
            else:
                return data
        except Exception as ex:
            raise KubeApiWatcherException("Failed to parse kubernetes api event", ex)

    @property
    def read_events_after(self):
        return self._since

    @property
    def create_response_stream(self) -> Callable:
        return self._create_response_stream

    def _watch_loop(self, *args, **kwargs):
        """FOR INTERNAL USE ONLY! Do not call this function outside the scope of this object.
        The main reader loop that emits data events form the kubernetes watch api.

        Emits:
            api_event
        """

        was_started = False
        reconnect_attempts = 0
        response_stream: HTTPResponse = None

        def can_attempt_reconnect(e: Exception):
            nonlocal reconnect_attempts
            reconnect_attempts += 1
            if reconnect_attempts == self.reconnect_max_retries:
                raise e
            self.emit("warning", self, e)
            sleep(self.reconnect_wait_timeout)

        # The execution params to use, kwargs is actually
        # the execution params passed to the kubernetes api call.
        if self.add_default_stream_params:
            # adding required streaming query values.
            kwargs["_preload_content"] = False
            kwargs["_request_timeout"] = self.response_wait_timeout

        def close_response_stream():
            response_stream.close()

        close_response_stream_event_handle = self.on(self._close_response_name_event_name, close_response_stream)

        # always read until stopped or resource dose not exist.
        try:
            while True:
                try:
                    if self.since is not None:
                        offset_read_seconds = (datetime.now() - self._last_read_time).seconds
                        kwargs["since_seconds"] = str(offset_read_seconds)

                    # closing any current response streams.
                    close_response_stream()

                    response_stream = self.create_response_stream(*args, **kwargs)
                    was_started = True

                    for line in _kube_api_lines_reader(response_stream):
                        # Is actually connected, therefore reading the response stream.
                        reconnect_attempts = 0
                        self._since = datetime.now()
                        self.emit(self.api_event_name, self.read_event(line))

                    # clean exit.
                    close_response_stream()

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
            close_response_stream()

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