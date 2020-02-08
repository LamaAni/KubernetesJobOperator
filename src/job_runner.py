import kubernetes
from .watchers.threaded_kubernetes_object_watchers import (
    ThreadedKubernetesNamespaceObjectsWatcher,
)
from .watchers.event_handler import EventHandler


class JobRunner(EventHandler):
    namespace_watcher: ThreadedKubernetesNamespaceObjectsWatcher = None
    client: kubernetes.client.CoreV1Api
    batchClient: kubernetes.client.BatchV1Api
    namespace: str = None

    def __init__(self, namespace):
        super().__init__()
        self.client = kubernetes.client.CoreV1Api()
        self.batchClient = kubernetes.client.BatchV1Api()
        self.namespace = namespace

        # creating the event watcher.
        self.namespace_watcher = ThreadedKubernetesNamespaceObjectsWatcher(self.client)
        self.namespace_watcher.pipe(self)

    def run(job_yaml):
        pass

