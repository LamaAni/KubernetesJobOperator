import kubernetes
import threading
from threading import Thread
from .event_handler import EventHandler
from .kubernetes_watch_stream import KubernetesWatchStream


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
    client: kubernetes.client.CoreV1Api = None

    def __init__(self, client, pod_name, namespace):
        super().__init__(client, lambda: self.log_reader(), return_type="raw")
        self.watcher.allow_stream_restart = False
        self.client = client
        self.pod_name = pod_name
        self.namespace = namespace

    def log_line(self, line):
        self.emit("log", line)

    def log_reader(self):
        def read_log(*args, **kwargs):
            val = self.client.read_namespaced_pod_log_with_http_info(
                self.pod_name, self.namespace, _preload_content=False, follow=True
            )

            return val[0]

        for line in self.watcher.stream(read_log):
            if line is not None:
                self.log_line(line)
            if self.is_stopped:
                break

        self.reset()

    def read_currnet_logs(self):
        log_lines = self.client.read_namespaced_pod_log(self.pod_name, self.namespace)
        if not isinstance(log_lines, list):
            log_lines = log_lines.split("\n")

        for possible_line in log_lines:
            for line in possible_line.split("\n"):
                self.log_line(line)


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
