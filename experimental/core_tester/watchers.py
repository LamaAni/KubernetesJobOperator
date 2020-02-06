import kubernetes
import threading
from threading import Thread
from typing import Dict
from urllib3.response import HTTPResponse


class KubernetesWatchStream(kubernetes.watch.Watch):
    _response_object: HTTPResponse = None

    def __init__(self, return_type=None):
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
        while True:
            resp = func(*args, **kwargs)
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
                resp.close()
                resp.release_conn()

            if timeouts or self._stop:
                break


class EventHandler:
    message_handlers: dict = None
    _pipeto: [] = None

    def __init__(self):
        super().__init__()
        self._pipeto = []
        self.message_handlers = dict()

    def on(self, name, handler):
        if not self.hasEvent(name):
            self.message_handlers[name] = []
        self.message_handlers[name].append(handler)

    def hasEvent(self, name):
        return name in self.message_handlers

    def clear(self, name):
        if self.hasEvent(name):
            del self.message_handlers[name]

    def emit(self, name, *args, **kwargs):
        if self.hasEvent(name):
            for handler in self.message_handlers[name]:
                handler(*args, **kwargs)
        for evnet_handler in self._pipeto:
            evnet_handler.emit(name, *args, **kwargs)

    def pipe(self, other):
        assert isinstance(other, EventHandler)
        self._pipeto.append(other)


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

    @property
    def is_stopped(self):
        return self.watcher._stop

    def start(self, method=None):
        self.emit("start")

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
            self.emit("log", msg=log_line)
            if self.is_stopped:
                break

        self.reset()


class ThreadedKubernetesNamespaceWatcher(ThreadedKubernetesWatcher):
    namespace: str = None
    field_selector: str = None
    label_selector: str = None
    _watch = None

    def __init__(self, client, namespace, field_selector=None, label_selector=None):
        super().__init__(client, lambda: self.read_pod_status())
        self.namespace = namespace
        self.field_selector = field_selector
        self.label_selector = label_selector

    def read_pod_status(self):
        def list_namespace_events(*args, **kwargs):
            if self.field_selector is not None:
                kwargs["field_selector"] = self.field_selector
            if self.label_selector is not None:
                kwargs["label_selector"] = self.label_selector
            (request, info, headers) = self.client.list_namespaced_event_with_http_info(
                self.namespace, _continue=True, **kwargs
            )
            return request

        for event in self.watcher.stream(list_namespace_events):
            self.emit("updated", event)
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
    _namespace_watchers: Dict[str, ThreadedKubernetesWatcher] = None
    auto_watch_pod_logs: bool = True

    def __init__(self):
        super().__init__()
        self._object_state = dict()
        self._watchers = dict()

    def get_kubernetes_object_id(obj: dict):
        pass

    def watch_namespace(
        self, namespace: str, label_selector: str = None, field_selector: str = None
    ):
        pass

    def waitfor(self, predict, include_log_events: bool = False):
        pass
