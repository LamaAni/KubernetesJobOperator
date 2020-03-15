import threading
import json
import re
from datetime import datetime
from time import sleep
from queue import SimpleQueue
from airflow_kubernetes_job_operator.event_handler import EventHandler
from urllib3.connectionpool import MaxRetryError, ReadTimeoutError, TimeoutError
from urllib3.response import HTTPResponse
import kubernetes
from kubernetes.stream.ws_client import ApiException


class ThreadedKubernetesWatchThreadEvent:
    event_type: str = None
    event_value = None

    def __init__(self, event_type, value=None):
        """A watch event, called from the reader thread.

        Arguments:

            event_type {str} -- The type of the event

        Keyword Arguments:

            value {any} -- The event value (default: {None})
        """
        super().__init__()
        self.event_type = event_type
        self.event_value = value


class ThreadedKubernetesWatch(EventHandler):
    _streaming_thread: threading.Thread = None
    _stream_queue: SimpleQueue = None
    _reader_response: HTTPResponse = None
    create_response_stream: callable = None
    read_as_dict: bool = True
    _thread_cleanly_exiting = False
    allow_reconnect: bool = True
    reconnect_max_retries: int = 20
    reconnect_wait_timeout: int = 5
    response_wait_timeout: int = 5
    add_default_stream_params: bool = True
    add_timing_stream_params: bool = True
    ignore_errors_if_removed: bool = True
    data_event_name: str = "data"
    _last_read_time: datetime = None
    thread_name: str = None

    def __init__(
        self,
        create_response_stream: callable,
        read_as_dict: bool = True,
        data_event_name: str = "data",
        watcher_thread_name: str = None,
    ):
        """Creates a threaded response reader, for the kubernetes API.
        Allows for partial readers, with continue on network disconnect.
        The stop command will kill the reader thread and associated connections.

        Arguments:

            create_response_stream {callable} -- The query create command

        Keyword Arguments:

            read_as_dict {bool} -- If true, the response value is a dictionary
            (as json), and should be parsed. (default: {True})
        """
        super().__init__()
        self._streaming_thread = None
        self._stream_queue = None
        self._reader_response = None
        self.create_response_stream = create_response_stream
        self.read_as_dict = read_as_dict
        self._thread_cleanly_exiting = False
        self.allow_reconnect = True
        self.reconnect_max_retries = 20
        self.reconnect_wait_timeout = 5
        self.response_wait_timeout = 5
        self.add_default_stream_params = True
        self.add_timing_stream_params = True
        self.ignore_errors_if_removed = True
        self.data_event_name = data_event_name
        self._last_read_time = None

        watcher_thread_name = watcher_thread_name or data_event_name

        self.thread_name = re.sub(r"[^a-z0-9_]", "_", watcher_thread_name.lower())

    @property
    def is_streaming(self):
        """True if currently reading events from the kubernetes cluster.

        Returns:
            bool -- True if streaming.
        """
        return self._streaming_thread is not None and self._streaming_thread.is_alive()

    def read_event(self, data):
        """Parses the event data.

        Arguments:

            data {str} -- The event data.

        Returns:

            dict/str -- Returns the parsed data, depending on read_as_dict.
        """
        if self.read_as_dict:
            return json.loads(data)
        else:
            return data

    def _invoke_threaded_event(self, event_type: str, event_value=None):
        """Call to emit an event.

        Arguments:

            event_type {str} -- The type of the event

        Keyword Arguments:

            event_value {any} -- The event value(default: {None})
        """
        self.emit(event_type, event_value)
        if self._stream_queue is not None:
            self._stream_queue.put(ThreadedKubernetesWatchThreadEvent(event_type, event_value))

    def _reader_thread(self, *args, **kwargs):
        """ FOR INTERNAL USE ONLY.
        The reader thread.

        Yields:

            str -- data line.
        """
        # define a line reader.
        def read_lines():
            prev = ""
            for seg in self._reader_response.read_chunked(decode_content=False):
                if isinstance(seg, bytes):
                    seg = seg.decode("utf8")
                seg = prev + seg
                lines = seg.split("\n")
                if not seg.endswith("\n"):
                    prev = lines[-1]
                    lines = lines[:-1]
                else:
                    prev = ""
                for line in lines:
                    if line:
                        yield line

        was_started = False
        reconnect_attempts = 0

        def reconnect_attempt(e: Exception):
            nonlocal reconnect_attempts
            reconnect_attempts += 1
            if reconnect_attempts == self.reconnect_max_retries:
                raise e
            self._invoke_threaded_event("warning", e)
            sleep(self.reconnect_wait_timeout)

        if self.add_default_stream_params:
            # adding required streaming query values.
            kwargs["_preload_content"] = False
            kwargs["_request_timeout"] = self.response_wait_timeout

        self._invoke_threaded_event("started")

        # always read until stopped or resource dose not exist.
        try:
            while True:
                try:
                    if self.add_timing_stream_params and self._last_read_time is not None:
                        offset_read_seconds = (datetime.now() - self._last_read_time).seconds
                        kwargs["since_seconds"] = str(offset_read_seconds)

                    # closing any current response streams.
                    self._close_response_stream(False)

                    self._reader_response = self.create_response_stream(*args, **kwargs)
                    reconnect_attempts = 0
                    was_started = True
                    # self._last_read_time = datetime.now()

                    for line in read_lines():
                        self._last_read_time = datetime.now()
                        kuberentes_event_data = self.read_event(line)
                        self._invoke_threaded_event(self.data_event_name, kuberentes_event_data)

                    # clean exit.
                    self._close_response_stream(False)

                except ApiException as e:

                    # exception_body = json.loads(e.body)
                    if e.reason == "Not Found" or e.reason == "Bad Request":
                        if was_started and self.ignore_errors_if_removed:
                            break
                        else:
                            raise e
                except MaxRetryError as e:
                    reconnect_attempt(e)
                except ReadTimeoutError as e:
                    reconnect_attempt(e)
                except TimeoutError as e:
                    reconnect_attempt(e)
                except Exception as e:
                    raise e

        except Exception as e:
            self._invoke_threaded_event("error", e)
        finally:
            self._close_response_stream(False)
            self._thread_cleanly_exiting = True
            self._invoke_threaded_event("stopped")

    def _start_streaming_thread(self, run_async=False, *args, **kwargs):
        """ FOR INTERNAL USE ONLY.
        Call to start the streaming thread.

        Keyword Arguments:

            run_async {bool} -- The final events will be reader
            run_async (default: {False})
        """
        if self.is_streaming:
            raise Exception("Already streaming, cannot start multiple streams")

        self._thread_cleanly_exiting = False
        self._stream_queue = SimpleQueue() if not run_async else None
        self._streaming_thread = threading.Thread(
            target=lambda args, kwargs: self._reader_thread(*args, **kwargs),
            args=(args, kwargs),
            name=f"kube_watcher_{self.thread_name}",
        )

        self._streaming_thread.start()

    def _clear_streaming_thread(self):
        """FOR INTERNAL USE ONLY.
        Clear the current executing thread, either
        by a clean stop (Wait for it) or destroy it.
        """
        if self._thread_cleanly_exiting:
            if self._streaming_thread.isAlive():
                self._streaming_thread.join()
        else:
            self._abort_thread()
        self._close_response_stream()
        self._streaming_thread = None
        self._stream_queue = None

    def stream(self, *args, **kwargs):
        """Stream the events from the kubernetes cluster, using yield.

        NOTE: Any arguments provided to this function will passed to the callable
        method provided in the constructor. (create_response_stream)

        Yields:

            any -- The event resource/str
        """

        # start the thread.
        self._start_streaming_thread(run_async=False, *args, **kwargs)

        try:
            while True:
                event = self._stream_queue.get()
                if not isinstance(event, ThreadedKubernetesWatchThreadEvent):
                    raise Exception(
                        "Invalid queue stream value type. Must be an instance of ThreadedKubernetesWatchThreadEvent"
                    )
                if event.event_type == self.data_event_name:
                    yield event.event_value
                elif event.event_type == "stopped":
                    break
                elif event.event_type == "error":
                    raise event.event_value

        except Exception as e:
            raise e
        finally:
            self._clear_streaming_thread()

    def start(self, *args, **kwargs):
        """Start the stream synchronically.
        You can read the events using the "on" method.
        """
        # start the thread.
        self._start_streaming_thread(run_async=True, *args, **kwargs)

    def _abort_thread(self):
        """Call to abort the current running thread.
        """
        if self._streaming_thread is not None:
            if self._streaming_thread.is_alive():
                # FIXME: Find a better way to stop the thread.
                self._streaming_thread._reset_internal_locks(False)
                self._streaming_thread._stop()

    def _close_response_stream(self, throw_errors=True):
        """Stop the current response, if any

        Keyword Arguments:
            throw_errors {bool} -- If true, throw errors from the stop process. (default: {True})
        """
        if self._reader_response is not None and not self._reader_response.isclosed():
            try:
                self._reader_response.close()
                self._reader_response.release_conn()
            except Exception as e:
                if throw_errors:
                    raise e
                self.emit("response_error", e)

    def stop(self):
        """Stop the read stream cleanly.
        """
        if self._stream_queue is not None:
            self._stream_queue.put(ThreadedKubernetesWatchThreadEvent("stop"))
        elif self.is_streaming:
            self.abort()

    def abort(self):
        """Force abort the response stream and thread.
        """
        self._abort_thread()
        self._streaming_thread = None
        self._stream_queue = None

    def join(self):
        if not self.is_streaming:
            raise Exception("Cannot join a non streaming watcher")
        self._streaming_thread.join()


class ThreadedKubernetesWatchPodLog(ThreadedKubernetesWatch):
    def __init__(self):
        """Create a threaded pod log reader, that will watch the pods
        for changes, and emit these as a 'log' event.

        Events:

            log - called on log.
            (other events inherited from ThreadedKubernetesWatch)
        """
        super().__init__(
            lambda *args, **kwargs: self._create_log_reader(*args, **kwargs), read_as_dict=False,
        )
        self.data_event_name = "log"

    def _create_log_reader(
        self, client: kubernetes.client.CoreV1Api, name: str, namespace: str, *args, **kwargs,
    ):
        """PRIVATE FOR INTERNAL USE ONLY.

        Arguments:

            client {kubernetes.client.CoreV1Api} -- The client
            name {str} -- The pod name
            namespace {str} -- The pod namespace.

        Returns:

            HTTPResponse -- The http response.
        """
        return client.read_namespaced_pod_log_with_http_info(
            name, namespace, follow=True, *args, **kwargs
        )[0]

    def stream(self, client: kubernetes.client.CoreV1Api, name: str, namespace: str):
        """Call to stream log events from a specific pod. Returns iterator.

        Arguments:

            client {kubernetes.client.CoreV1Api} -- The kubernetes client
            name {str} -- The pod
            namespace {str} -- The pod namespace.

        Returns:

            iterator(str) -- The pod log iterator.
        """
        self.thread_name = f"{namespace}_Pod_{name}"
        return ThreadedKubernetesWatch.stream(self, client=client, name=name, namespace=namespace)

    def start(self, client: kubernetes.client.CoreV1Api, name: str, namespace: str):
        """Starts the pod log reader asynchronically.

        Arguments:

            client {kubernetes.client.CoreV1Api} -- [description]
            name {str} -- [description]
            namespace {str} -- [description]

        Returns:

            [type] -- [description]
        """
        self.thread_name = f"{namespace}_Pod_{name}"
        return ThreadedKubernetesWatch.start(self, client=client, name=name, namespace=namespace)

    def read_currnet_logs(self, client: kubernetes.client.CoreV1Api, name: str, namespace: str):
        """Read all of the current logs from the resource. Helper method.

        Arguments:

            client {kubernetes.client.CoreV1Api} -- The kube client
            name {str} -- The pod name
            namespace {str} -- The namespace
        """
        log_lines = client.read_namespaced_pod_log(name, namespace)
        if not isinstance(log_lines, list):
            log_lines = log_lines.strip().split("\n")

        for possible_line in log_lines:
            for line in possible_line.split("\n"):
                self._invoke_threaded_event(self.data_event_name, self.read_event(line))


class ThreadedKubernetesWatchNamspeace(ThreadedKubernetesWatch):
    def __init__(self):
        super().__init__(
            lambda *args, **kwargs: self._create_namespace_watcher(*args, **kwargs),
            read_as_dict=True,
        )
        self.data_event_name = "update"

    def _kind_to_watch_uri(self, namespace, kind):
        if kind == "Pod":
            return f"/api/v1/namespaces/{namespace}/pods"
        elif kind == "Job":
            return f"/apis/batch/v1/namespaces/{namespace}/jobs"
        elif kind == "Service":
            return f"/api/v1/namespaces/{namespace}/services"
        elif kind == "Deployment":
            return f"/apis/apps/v1beta2/namespaces/{namespace}/deployments"
        elif kind == "Event":
            return f"/api/v1/namespaces/{namespace}/events"
        raise Exception("Watch type not found: " + kind)

    def _create_namespace_watcher(
        self,
        client: kubernetes.client.CoreV1Api,
        namespace: str,
        kind: str,
        field_selector: str = None,
        label_selector: str = None,
        *args,
        **kwargs,
    ):
        path_params = {"namespace": namespace}
        query_params = {
            "pretty": False,
            "_continue": True,
            "fieldSelector": field_selector or "",
            "labelSelector": label_selector or "",
            "watch": True,
        }

        # watch request, default values.
        body_params = None
        local_var_files = {}
        form_params = []
        collection_formats = {}

        # Set header
        header_params = {}
        header_params["Accept"] = client.api_client.select_header_accept(
            [
                "application/json",
                "application/yaml",
                "application/vnd.kubernetes.protobuf",
                "application/json;stream=watch",
                "application/vnd.kubernetes.protobuf;stream=watch",
            ]
        )
        header_params["Content-Type"] = client.api_client.select_header_content_type(["*/*"])

        # Authentication
        auth_settings = ["BearerToken"]

        api_call = client.api_client.call_api(
            self._kind_to_watch_uri(namespace, kind),
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

    def stream(
        self,
        client: kubernetes.client.CoreV1Api,
        kind: str,
        namespace: str,
        field_selector: str = None,
        label_selector: str = None,
    ):
        """Streams events from the kubernetes namespace

        Arguments:

            client {kubernetes.client.CoreV1Api} -- client
            kind {str} -- The resources events to watch
            namespace {str} -- The namespace

        Keyword Arguments:

            field_selector {str} -- The field select to filter the resources (default: {None})
            label_selector {str} -- The label selector to filter the resource (default: {None})

        Yields:

            dict - the event yaml dictionary.
        """
        self.thread_name = f"{namespace}__list_{kind}"
        return ThreadedKubernetesWatch.stream(
            self,
            client=client,
            namespace=namespace,
            kind=kind,
            field_selector=field_selector,
            label_selector=label_selector,
        )

    def start(
        self,
        client: kubernetes.client.CoreV1Api,
        kind: str,
        namespace: str,
        field_selector: str = None,
        label_selector: str = None,
    ):
        """Starts the namespace watcher asnyc

        Arguments:

            client {kubernetes.client.CoreV1Api} -- The kube client
            namespace {str} -- The namespace
            kind {str} -- The resource kind to watch ("Pod","Job"...)

        Keyword Arguments:

            field_selector {str} -- The field select to filter the resources (default: {None})
            label_selector {str} -- The label selector to filter the resource (default: {None})
        """
        self.thread_name = f"{namespace}_list_{kind}"
        ThreadedKubernetesWatch.start(
            self,
            client=client,
            namespace=namespace,
            kind=kind,
            field_selector=field_selector,
            label_selector=label_selector,
        )

