import kubernetes
import threading


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
    active_log_read_thread = None

    def __init__(self, client, pod_name, namespace, on_message):
        super().__init__()
        self.on_message = on_message
        self.client = client
        self.pod_name = pod_name
        self.namespace = namespace

    def start():
        pass
