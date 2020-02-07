import kubernetes
import threading
from threading import Thread
from typing import Dict, List
from urllib3.response import HTTPResponse
from queue import SimpleQueue


class EventHandler:
    message_handlers: Dict[str, Dict[int, object]] = None
    _pipeto: [] = None
    _handler_last_idx = 0

    def __init__(self):
        super().__init__()
        self._pipeto = []
        self.message_handlers = dict()
        self._handler_last_idx = 0

    def on(self, name, handler):
        if not self.hasEvent(name):
            self.message_handlers[name] = dict()
        idx = self._handler_last_idx
        self._handler_last_idx += 1

        self.message_handlers[name][idx] = handler
        return idx

    def hasEvent(self, name):
        return name in self.message_handlers

    def clear(self, name, idx: int = None):
        if self.hasEvent(name):
            if idx is not None:
                del self.message_handlers[name][idx]
            else:
                del self.message_handlers[name]

    def emit(self, name, *args, **kwargs):
        if self.hasEvent(name):
            for handler in self.message_handlers[name].values():
                handler(*args, **kwargs)
        for evnet_handler in self._pipeto:
            evnet_handler.emit(name, *args, **kwargs)

    def pipe(self, other):
        assert isinstance(other, EventHandler)
        self._pipeto.append(other)


class KubernetesWatchStream(kubernetes.watch.Watch, EventHandler):
    _response_object: HTTPResponse = None

    def __init__(self, return_type=None):
        EventHandler.__init__(self)
        super().__init__(return_type=return_type)

    def read_lines(self, resp: HTTPResponse):
        prev = ""
        for seg in resp.read_chunked(decode_content=False):
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

    def stream(self, func, *args, **kwargs):
        """Watch an API resource and stream the result back via a generator.

        :param func: The API function pointer. Any parameter to the function
                     can be passed after this parameter.

        :return: Event object with these keys:
                   'type': The type of event such as "ADDED", "DELETED", etc.
                   'raw_object': a dict representing the watched object.
                   'object': A model representation of raw_object. The name of
                             model will be determined based on
                             the func's doc string. If it cannot be determined,
                             'object' value will be the same as 'raw_object'.
        """

        self._stop = False
        return_type = (
            None if self._raw_return_type == "raw" else self.get_return_type(func)
        )
        kwargs["watch"] = True
        kwargs["_preload_content"] = False
        if "resource_version" in kwargs:
            self.resource_version = kwargs["resource_version"]

        timeouts = "timeout_seconds" in kwargs
        was_started = False
        self.emit("start")
        while True:
            try:
                resp = func(*args, **kwargs)
            except Exception as e:
                if e.reason == "Not Found" and was_started:
                    # pod was deleted, or removed, clean exit.
                    break
                else:
                    raise e

            if not was_started:
                self.emit("response_started")
                was_started = True

            self._response_object = resp
            try:
                lines_reader = self.read_lines(resp)
                for data in lines_reader:
                    if len(data.strip()) > 0:
                        yield data if return_type is None else self.unmarshal_event(
                            data, return_type
                        )
                    if self._stop:
                        break
            finally:
                self._response_object = None
                kwargs["resource_version"] = self.resource_version
                if resp is not None:
                    resp.close()
                    resp.release_conn()

            if timeouts or self._stop:
                break
        self.emit("stopped")


class ThreadedKubernetesWatcher(EventHandler):
    client: kubernetes.client.CoreV1Api = None
    _active_log_read_thread: Thread = None
    _is_stopped: bool = False
    _invoke_method = None
    watcher: KubernetesWatchStream = None

    def __init__(self, client, invoke_method, return_type=None):
        super().__init__()
        assert client is not None
        self.client = client
        assert invoke_method is not None
        self._invoke_method = invoke_method
        self.watcher = KubernetesWatchStream(return_type)
        self.watcher.pipe(self)

    @property
    def is_stopped(self):
        return self.watcher._stop

    def start(self, method=None):
        method = method or self._invoke_method

        if self._active_log_read_thread is not None:
            raise Exception("Log reader has already been started.")

        self._active_log_read_thread = threading.Thread(target=self._invoke_method)
        self._active_log_read_thread.start()

    def reset(self):
        self.stop()

    def stop(self):
        if self.is_stopped:
            return

        self.watcher.stop()

        # FIXME: This approach is good as long as _stop is available.
        # maybe there is another way to kill the thread?
        if (
            self._active_log_read_thread is not None
            and threading.currentThread().ident != self._active_log_read_thread.ident
        ):
            try:
                # Wait for clean finish after marking watcher as stopped
                self._active_log_read_thread.join(0.1)
            except Exception:
                pass

            if self._active_log_read_thread.isAlive():
                # force stop.
                self._active_log_read_thread._reset_internal_locks(False)
                self._active_log_read_thread._stop()
                self.emit("stopped")

        self._active_log_read_thread = None

    def join(self, timeout: float = None):
        self.emit("join")
        if self._active_log_read_thread is None:
            raise Exception("Logger must be started before it can be joined")

        self._active_log_read_thread.join(timeout)


class ThreadedKuebrnetesLogReader(ThreadedKubernetesWatcher):
    pod_name: str = None
    namespace: str = None

    def __init__(self, client, pod_name, namespace):
        super().__init__(client, lambda: self.log_reader(), return_type="raw")
        self.client = client
        self.pod_name = pod_name
        self.namespace = namespace

    def log_reader(self):
        def read_log(*args, **kwargs):
            val = self.client.read_namespaced_pod_log_with_http_info(
                self.pod_name, self.namespace, _preload_content=False, follow=True
            )

            return val[0]

        for log_line in self.watcher.stream(read_log):
            if log_line is not None:
                self.emit("log", msg=log_line)
            if self.is_stopped:
                break

        self.reset()


class ThreadedKubernetesNamespaceWatcher(ThreadedKubernetesWatcher):
    namespace: str = None
    field_selector: str = None
    label_selector: str = None
    _watch = None
    _watch_type: str = None

    def __init__(
        self,
        watch_type: str,
        client: kubernetes.client.CoreV1Api,
        namespace: str,
        field_selector: str = None,
        label_selector: str = None,
    ):
        super().__init__(client, lambda: self.read_watch_events())
        self.namespace = namespace
        self.field_selector = field_selector
        self.label_selector = label_selector
        self._watch_type = watch_type

    def __watch_type_to_uri(self):
        if self._watch_type == "pod":
            return f"/api/v1/namespaces/{self.namespace}/pods"
        elif self._watch_type == "job":
            return f"/apis/batch/v1/namespaces/{self.namespace}/jobs"
        elif self._watch_type == "service":
            return f"/api/v1/namespaces/{self.namespace}/services"
        elif self._watch_type == "deployment":
            return f"/apis/apps/v1beta2/namespaces/{self.namespace}/deployments"
        elif self._watch_type == "event":
            return f"/api/v1/namespaces/{self.namespace}/events"
        raise Exception("Watch type not found: " + self._watch_type)

    def __get_kube_objects_list_watch_response(self):

        path_params = {"namespace": self.namespace}
        query_params = {
            "include_uninitialized": False,
            "pretty": False,
            "_continue": True,
            "field_selector": self.field_selector or "",
            "label_selector": self.label_selector or "",
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
        header_params[
            "Content-Type"
        ] = self.client.api_client.select_header_content_type(["*/*"])

        # Authentication
        auth_settings = ["BearerToken"]

        api_call = self.client.api_client.call_api(
            self.__watch_type_to_uri(),
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

    def read_watch_events(self):
        def list_namespace_events(*args, **kwargs):
            return self.__get_kube_objects_list_watch_response()

        for event in self.watcher.stream(list_namespace_events):
            self.emit("update", event)
            self.emit(event["type"], event)
            if self.is_stopped:
                break


class ThreadedKubernetesNamespaceObjectWatchStatus:
    object_type: str = None
    object_yaml: str = None
    status: str = None
    log_reader: ThreadedKuebrnetesLogReader = None

    def __init__(self, object_yaml, auto_watch_pod_logs: bool = True):
        super().__init__()

    def stop(self):
        if self.log_reader is not None and not self.log_reader.is_stopped:
            self.log_reader.stop()


class ThreadedKubernetesNamespaceObjectWatcher(EventHandler):
    _object_state: Dict[str, ThreadedKubernetesNamespaceObjectWatchStatus] = None
    _namespace_watchers: Dict[str, List[ThreadedKubernetesWatcher]] = None
    auto_watch_pod_logs: bool = True
    client: kubernetes.client.CoreV1Api = None

    def __init__(self, client: kubernetes.client.CoreV1Api):
        super().__init__()
        self.client = client
        self._object_state = dict()
        self._namespace_watchers = dict()

    def watch_namespace(
        self, namespace: str, label_selector: str = None, field_selector: str = None
    ):
        assert isinstance(namespace, str) and len(namespace) > 0
        if namespace in self._namespace_watchers:
            raise Exception("Namespace already being watched.")

        watchers = []
        self._namespace_watchers[namespace] = watchers
        for watch_type in ["pod", "job", "deployment", "service"]:
            watcher = ThreadedKubernetesNamespaceWatcher(
                watch_type, self.client, namespace
            )
            watcher.pipe(self)
            watcher.start()
            watchers.append(watcher)

    def emit(self, name, *args, **kwargs):
        if name == "update":
            self.update_object(args[0])

        super().emit(name, *args, **kwargs)

    def update_object(self, event):
        kube_object = event["object"]
        pass

    def waitfor(self, predict, include_log_events: bool = False, timeout: float = None):
        class wait_event:
            event: object = None
            sender: object = None

            def __init__(self, event, sender):
                super().__init__()
                self.event = event
                self.sender = sender

        event_queue = SimpleQueue()

        def add_queue_event(event, sender=None):
            event_queue.put(wait_event(event, sender))

        event_handler_idx = self.on("update", add_queue_event)

        while True:
            info = event_queue.get(timeout=timeout)
            if predict(info.event, info.sender):
                break

        self.clear("update", event_handler_idx)

    def waitfor_pod_status(
        self,
        pod_name: str,
        namespace: str,
        phase: str = None,
        predict=None,
        timeout: float = None,
    ):
        assert phase is not None or predict is not None

        def default_predict(status, event):
            return status["phase"] == phase

        predict = predict or default_predict

        def wait_predict(event, sender=None):
            pod_obj = event["object"]
            if (
                pod_obj["metadata"]["name"] != pod_name
                or pod_obj["metadata"]["namespace"] != namespace
            ):
                return

            status = pod_obj["status"]
            return predict(status, event)

        self.waitfor(wait_predict, False, timeout)

    def stop(self):
        for namespace, watchers in self._namespace_watchers.items():
            for watcher in watchers:
                watcher.stop()
