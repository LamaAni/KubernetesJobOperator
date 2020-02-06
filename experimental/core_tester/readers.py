import kubernetes
import threading
from threading import Thread


class KubernetesWatchRawStream(kubernetes.watch.Watch):
    @staticmethod
    def iter_resp_lines(resp):
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
        kwargs["watch"] = True
        kwargs["_preload_content"] = False
        if "resource_version" in kwargs:
            self.resource_version = kwargs["resource_version"]

        timeouts = "timeout_seconds" in kwargs
        while True:
            resp = func(*args, **kwargs)
            try:
                for line in KubernetesWatchRawStream.iter_resp_lines(resp):
                    yield line
                    if self._stop:
                        break
            finally:
                kwargs["resource_version"] = self.resource_version
                resp.close()
                resp.release_conn()

            if timeouts or self._stop:
                break


class EventHandler:
    message_handlers: dict = None

    def __init__(self):
        super().__init__()
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
        if not self.hasEvent(name):
            return
        for handler in self.message_handlers[name]:
            handler(*args, **kwargs)


class ThreadedKuebrnetesLogReader(EventHandler):
    namespace: str = None
    pod_name: str = None
    client: kubernetes.client.CoreV1Api = None
    _active_log_read_thread: Thread = None
    _is_stopped: bool = False

    def __init__(self, client, pod_name, namespace):
        super().__init__()
        self.client = client
        self.pod_name = pod_name
        self.namespace = namespace

    def start(self):
        if self._active_log_read_thread is not None:
            raise Exception("Log reader has already been started.")

        def read_log(*args, **kwargs):
            val = self.client.read_namespaced_pod_log_with_http_info(
                self.pod_name, self.namespace, _preload_content=False, follow=True
            )
            return val[0]

        def log_reader():
            for log_line in KubernetesWatchRawStream().stream(read_log):
                self.emit("log", msg=log_line)
                if self._is_stopped:
                    break
            self._active_log_read_thread = None

        self._is_stopped = False
        self._active_log_read_thread = threading.Thread(target=log_reader)
        self._active_log_read_thread.start()

    def stop(self):
        self._is_stopped = True

    def abort(self):
        if (
            self._active_log_read_thread is not None
            and self._active_log_read_thread.isAlive()
        ):
            self._active_log_read_thread._stop()

    def join(self, timeout: float = None):
        if self._active_log_read_thread is None:
            raise Exception("Logger must be started before it can be joined")

        self._active_log_read_thread.join(timeout)
