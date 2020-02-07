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
            cur_handlers = list(self.message_handlers[name].values())
            for handler in cur_handlers:
                handler(*args, **kwargs)
        for evnet_handler in self._pipeto:
            evnet_handler.emit(name, *args, **kwargs)

    def pipe(self, other):
        assert isinstance(other, EventHandler)
        self._pipeto.append(other)


class KubernetesWatchStream(kubernetes.watch.Watch, EventHandler):
    _response_object: HTTPResponse = None
    allow_stream_restart = True

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
                if (
                    e.reason == "Not Found" or "is terminated" in str(e.body)
                ) and was_started:
                    # pod was deleted, or removed, clean exit.
                    break
                else:
                    raise e

            if was_started and not self.allow_stream_restart:
                break

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


class ThreadedKubernetesObjectsWatcher(EventHandler):
    _object_yaml: str = None
    auto_watch_pod_logs: bool = True
    _log_reader: ThreadedKuebrnetesLogReader = None
    _id: str = None
    _has_read_logs = False
    client: kubernetes.client.CoreV1Api = None

    def __init__(self, client, object_yaml, auto_watch_pod_logs: bool = True):
        super().__init__()
        self.client = client
        self._id = ThreadedKubernetesObjectsWatcher.compose_object_id(object_yaml)
        self.auto_watch_pod_logs = auto_watch_pod_logs

    def emit(self, name, *args, **kwargs):
        if len(args) < 2:
            args = list(args) + [self]
        super().emit(name, *args, **kwargs)

    def stop(self):
        if self._log_reader is not None and not self._log_reader.is_stopped:
            self._log_reader.stop()

    @property
    def id(self):
        return self._id

    @property
    def kind(self):
        return self.yaml["kind"]

    @property
    def yaml(self) -> dict:
        return self._object_yaml

    @property
    def name(self) -> str:
        return self.yaml["metadata"]["name"]

    @property
    def namespace(self) -> str:
        return self.yaml["metadata"]["namespace"]

    @staticmethod
    def compose_object_id(object_yaml):
        return "/".join(
            [
                object_yaml["kind"],
                object_yaml["metadata"]["namespace"],
                object_yaml["metadata"]["name"],
            ]
        )

    @property
    def status(self):
        if self.kind == "Service":
            return "Deleted" if self.was_deleted else "Active"
        elif self.kind == "Job":
            job_status = None
            if self.was_deleted:
                job_status = "Deleted"
            elif (
                "startTime" in self.yaml["status"]
            ):
                if "completionTime" in self.yaml["status"]:
                    job_status = "Succeeded"
                else:
                    job_status = "Running"
            else:
                job_status = "Pending"

            return job_status
        elif self.kind == "Pod":
            pod_status = None
            if self.was_deleted:
                pod_status = "Deleted"
            else:
                pod_status = self.yaml["status"]["phase"]
            if "status" in self.yaml and "containerStatuses" in self.yaml["status"]:
                for container_status in self.yaml["status"]["containerStatuses"]:
                    if "state" in container_status:
                        if (
                            "waiting" in container_status["state"]
                            and "reason" in container_status["state"]["waiting"]
                            and "BackOff"
                            in container_status["state"]["waiting"]["reason"]
                        ):
                            return "Error"
                        if "error" in container_status["state"]:
                            return "Error"

            return pod_status
        else:
            raise Exception("Not implemented for kind: " + self.kind)

    def update_pod_state(self, event_type, old_status):
        # Called to update a current pod state.
        # may start/read pod logs.
        cur_status = self.status
        need_read_pod_logs = (
            cur_status and cur_status != "Pending" and not self._has_read_logs
        )
        read_pod_static_logs = self.status != "Running"

        if need_read_pod_logs:
            self._log_reader = ThreadedKuebrnetesLogReader(
                self.client, self.name, self.namespace
            )
            self._log_reader.pipe(self)
            self._has_read_logs = True
            if read_pod_static_logs:
                # in case where the logs were too fast,
                # and they need to be read sync.
                self._log_reader.read_currnet_logs()
                self._log_reader = None
                pass
            else:
                # async read logs.
                self._log_reader.start()

    def update_object_state(self, event_type, object_yaml: dict):
        is_new = self._object_yaml is None
        self.was_deleted = event_type == "DELETED"
        old_status = None if is_new else self.status

        # update the current yaml.
        self._object_yaml = object_yaml

        if self.kind == "Pod":
            self.update_pod_state(event_type, old_status)

        if old_status != self.status:
            self.emit("status", self.status, self)


class ThreadedKubernetesNamespaceObjectsWatcher(EventHandler):
    _object_watchers: Dict[str, ThreadedKubernetesObjectsWatcher] = None
    _namespace_watchers: Dict[str, List[ThreadedKubernetesWatcher]] = None
    auto_watch_pod_logs: bool = True
    remove_deleted_kube_objects_from_memory: bool = True
    client: kubernetes.client.CoreV1Api = None

    def __init__(self, client: kubernetes.client.CoreV1Api):
        super().__init__()
        self.client = client
        self._object_watchers = dict()
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
        if name == "DELETED":
            self.delete_object(args[0])
        elif name == "update":
            self.update_object(args[0])

        if len(args) < 2:
            args = list(args) + [self]

        super().emit(name, *args, **kwargs)

    def update_object(self, event):
        kube_object = event["object"]
        event_type = event["type"]
        oid = ThreadedKubernetesObjectsWatcher.compose_object_id(kube_object)
        if oid not in self._object_watchers:
            if event_type == "DELETED":
                return
            self._object_watchers[oid] = ThreadedKubernetesObjectsWatcher(
                self.client, kube_object, self.auto_watch_pod_logs
            )
            self._object_watchers[oid].pipe(self)

        self._object_watchers[oid].update_object_state(event_type, kube_object)

    def delete_object(self, event):
        # first update the current object.
        self.update_object(event)

        kube_object = event["object"]
        oid = ThreadedKubernetesObjectsWatcher.compose_object_id(kube_object)
        if oid in self._object_watchers:
            watcher = self._object_watchers[oid]
            watcher.stop()
            if self.remove_deleted_kube_objects_from_memory:
                del self._object_watchers[oid]

    def waitfor(
        self,
        predict,
        include_log_events: bool = False,
        timeout: float = None,
        event: str = "update",
    ):
        class wait_event:
            args: list = None
            kwargs: dict = None

            def __init__(self, args, kwargs):
                super().__init__()
                self.args = args
                self.kwargs = kwargs

        event_queue = SimpleQueue()

        def add_queue_event(*args, **kwargs):
            event_queue.put(wait_event(list(args), dict(kwargs)))

        event_handler_idx = self.on(event, add_queue_event)

        info = None
        while True:
            info = event_queue.get(timeout=timeout)
            args = info.args
            kwargs = info.kwargs
            if predict(*args, **kwargs):
                break

        self.clear("update", event_handler_idx)
        return None if info is None else info.args[-1]

    def waitfor_status(
        self,
        kind: str = None,
        name: str = None,
        namespace: str = None,
        status: str = None,
        status_list: List[str] = None,
        predict=None,
        timeout: float = None,
    ):
        assert (
            status is not None
            or (status_list is not None and len(status_list) > 0)
            or predict is not None
        )

        def default_predict(
            match_status: str, sender: ThreadedKubernetesObjectsWatcher
        ):
            if status is not None and status == match_status:
                return True
            if status_list is not None:
                for sli in status_list:
                    if sli == match_status:
                        return True
            return False

        predict = predict or default_predict

        def wait_predict(status, sender: ThreadedKubernetesObjectsWatcher):
            if name is not None and sender.name != name:
                return False
            if namespace is not None and sender.namespace != namespace:
                return False
            if kind is not None and sender.kind != kind:
                return False
            return predict(status, sender)

        return self.waitfor(wait_predict, False, timeout=timeout, event="status")

    def stop(self):
        for namespace, watchers in self._namespace_watchers.items():
            for watcher in watchers:
                watcher.stop()

        for oid, owatch in self._object_watchers.items():
            owatch.stop()
