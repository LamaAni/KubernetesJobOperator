import kubernetes
import threading
from threading import Thread


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

    def __init__(self, client, pod_name, namespace, on_message):
        super().__init__()
        self.on_message = on_message
        self.client = client
        self.pod_name = pod_name
        self.namespace = namespace

    def start(self):
        if self.active_log_read_thread is not None:
            raise Exception("Log reader has already been started.")

        def read_log(*args, **kwargs):
            val = self.client.read_namespaced_pod_log_with_http_info(
                self.pod_name, self.namespace, _preload_content=False, follow=True
            )
            return val[0]

        def log_reader(reader: ThreadedKuebrnetesLogReader):
            for log_line in kubernetes.watch.Watch().stream(read_log):
                self.emit("log", msg=log_reader)

        self._active_log_read_thread = threading.Thread(target=log_reader, args=(self))

    def stop(self):
        self._is_stopped = True

    def abort(self):
        if (
            self.active_log_read_thread is not None
            and self.active_log_read_thread.isAlive()
        ):
            self._active_log_read_thread._stop()
