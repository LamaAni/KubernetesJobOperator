# Implements a rest api
from logging import Logger
import os
import json
import traceback
from os import terminal_size
from socket import timeout
from types import TracebackType
import yaml
from typing import List, Callable, Set
from weakref import WeakSet
from os.path import expanduser
from zthreading.tasks import Task
from zthreading.events import Event, EventHandler
from requests import request, Response

from airflow_kubernetes_job_operator.kube_api.exceptions import KubeApiException
from airflow_kubernetes_job_operator.kube_api.utils import unqiue_with_order, clean_dictionary_nulls

from kubernetes.client import ApiClient
from kubernetes.config import kube_config, incluster_config, list_kube_config_contexts
from airflow_kubernetes_job_operator.kube_api.utils import kube_logger


def join_locations_list(*args):
    lcoations = []
    v = None
    for v in args:
        if v is None or (isinstance(v, str) and len(v.strip()) == ""):
            continue
        if isinstance(v, list):
            lcoations += v
        else:
            lcoations += v.split(",")
    return unqiue_with_order(lcoations)


KUBERNETES_JOB_OPERATOR_DEFAULT_CONFIG_LOCATIONS = join_locations_list(
    [kube_config.KUBE_CONFIG_DEFAULT_LOCATION], os.environ.get("KUBERNETES_JOB_OPERATOR_DEFAULT_CONFIG_LOCATIONS", None)
)
KUBERNETES_SERVICE_ACCOUNT_PATH = os.path.dirname(incluster_config.SERVICE_CERT_FILENAME)
KUBERNETES_API_CLIENT_USE_ASYNCIO_ENV_NAME = "KUBERNETES_API_CLIENT_USE_ASYNCIO"


def set_asyncio_mode(is_active: bool):
    if is_active:
        raise NotImplementedError("AsyncIO not yet implemented.")
    KubeApiRestQuery.default_use_asyncio = is_active


def get_asyncio_mode() -> bool:
    """Returns the asyncio mode. Defaults to true if not defined in env (reduces memory)"""
    return KubeApiRestQuery.default_use_asyncio


if "KUBERNETES_API_CLIENT_USE_ASYNCIO_ENV_NAME" in os.environ:
    set_asyncio_mode(os.environ.get("KUBERNETES_API_CLIENT_USE_ASYNCIO_ENV_NAME", "false") == "true")


class KubeApiRestQuery(Task):
    data_event_name = "kube_api_query_result"
    default_use_asyncio = False

    query_started_event_name = "kube_api_query_started"
    query_ended_event_name = "kube_api_query_ended"

    def __init__(
        self,
        resource_path: str,
        method: str = "GET",
        path_params: dict = None,
        query_params: dict = None,
        form_params: list = None,
        files: dict = None,
        body: dict = None,
        headers: dict = None,
        timeout: float = None,
        use_asyncio: bool = None,
    ):
        assert use_asyncio is not True, NotImplementedError("AsyncIO not yet implemented.")
        super().__init__(
            self._exdcute_query,
            use_async_loop=use_asyncio or KubeApiRestQuery.default_use_asyncio,
            use_daemon_thread=True,
            thread_name=f"{self.__class__.__name__} {id(self)}",
        )

        self.resource_path = resource_path
        self.timeout = timeout
        self.path_params = path_params or dict()
        self.query_params = query_params or dict()
        self.form_params = form_params
        self.headers = headers
        self.method = method
        self.files = files
        self.body = body

        # these event are object specific
        self.query_started_event_name = f"{self.query_started_event_name} {id(self)}"
        self.query_ended_event_name = f"{self.query_started_event_name} {id(self)}"

    def parse_data(self, line: str):
        return line

    def emit_data(self, data):
        self.emit(self.data_event_name, data)

    @classmethod
    def read_response_stream_lines(cls, response):
        """INTERNAL. Helper yield method. Parses the streaming http response
        to lines (can by async!)

        Yields:
            str: The line
        """
        prev = ""
        for chunk in response.stream(decode_content=False):
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

    def pre_request(self, client: "KubeApiRestClient"):
        pass

    def post_request(self, client: "KubeApiRestClient"):
        pass

    def _exdcute_query(self, client: "KubeApiRestClient"):
        self.emit(self.query_started_event_name, self, client)
        self.pre_request(client)
        self.query_loop(client)
        self.post_request(client)
        self.emit(self.query_ended_event_name, self, client)

    def query_loop(self, client: "KubeApiRestClient"):
        def validate_dictionary(d: dict, default: dict = None):
            if default is not None:
                update_with = d or {}
                d = default.copy()
                d.update(update_with)
            else:
                d = (d or {}).copy()

            return clean_dictionary_nulls(d)

        headers = validate_dictionary(
            self.headers,
            default={
                "Accept": client.api_client.select_header_accept(["*/*"]),
                "Content-Type": client.api_client.select_header_content_type(["*/*"]),
            },
        )

        path_params = validate_dictionary(self.path_params)
        query_params = validate_dictionary(self.query_params)
        form_params = validate_dictionary(self.form_params)
        files = validate_dictionary(self.files)

        query_params_array = []
        for k in query_params:
            query_params_array.append((k, query_params[k]))

        # starting query.
        request_info = client.api_client.call_api(
            resource_path=self.resource_path,
            method=self.method,
            path_params=clean_dictionary_nulls(path_params),
            query_params=clean_dictionary_nulls(query_params),
            header_params=clean_dictionary_nulls(headers),
            body=self.body,
            post_params=form_params or [],
            files=files or dict(),
            response_type="str",
            auth_settings=["BearerToken"],
            async_req=False,
            _return_http_data_only=False,
            _preload_content=False,
            _request_timeout=self.timeout,
            collection_formats={},
        )

        response = request_info[0]

        for line in self.read_response_stream_lines(response):
            data = self.parse_data(line)
            self.emit_data(data)

    def start(self, client: "KubeApiRestClient"):
        """Start the query execution

        Args:
            client (ApiClient): The api client to use.
        """
        assert not self.is_running, "Cannot start a running query"
        assert isinstance(client, KubeApiRestClient), "client must be of class KubeApiRestClient"

        super().start(client)

        return self

    def wait_until_started(self, timeout: float = 5, ignore_if_running=True):
        if self.is_running and ignore_if_running:
            return
        self.wait_for(self.query_started_event_name, timeout=timeout)

    def stop(self, timeout: float = None, throw_error_if_not_running: bool = None):
        self.stop_all_streams()
        return super().stop(timeout=timeout, throw_error_if_not_running=throw_error_if_not_running)

    def log_event(self, logger: Logger, ev: Event):
        pass

    def bind_logger(self, logger: Logger = kube_logger, allowed_event_names=None) -> int:
        allowed_event_names = set(allowed_event_names or [self.data_event_name])

        def process_log_event(ev: Event):
            if ev.name in [self.error_event_name, self.warning_event_name]:
                err: Exception = ev.args[-1] if len(ev.args) > 0 else Exception("Unknown error")
                msg = (
                    "\n".join(traceback.format_exception(err.__class__, err, err.__traceback__))
                    if isinstance(err, Exception)
                    else err
                )
                if ev.name == self.error_event_name:
                    logger.error(msg)
                else:
                    logger.warning(msg)
            elif ev.name in allowed_event_names:
                self.log_event(logger, ev)

        # bind errors and warnings.
        return self.on_any_event(lambda name, *args, **kwargs: process_log_event(Event(name, args, kwargs)))


class KubeApiRestClient:
    def __init__(
        self,
        config_file: str = None,
        is_in_cluster: bool = None,
        extra_config_locations: List[str] = None,
        context: str = None,
        persist: bool = False,
    ):
        """Creates a new kubernetes api rest client.

        Args:
            config_file (str, optional): The configuration file path. Defaults to None = search for config.
            is_in_cluster (bool, optional): If true, the client will expect to run inside a cluster
                and to load the cluster config. Defaults to None = auto detect.
            extra_config_locations (List[str], optional): Extra locations to search for a configuration. Defaults to None.
            context (str, optional): The context name to run in. Defaults to None = active context.
            persist (bool, optional): If True, config file will be updated when changed
                (e.g GCP token refresh).
        """
        super().__init__()
        self.kube_config: kube_config.Configuration = self.load_kube_config(
            config_file, is_in_cluster, extra_config_locations, context, persist
        )
        self.api_client: ApiClient = ApiClient(configuration=self.kube_config)
        self._active_queries: Set[KubeApiRestQuery] = WeakSet()
        self._active_handlers: Set[EventHandler] = WeakSet()

    @classmethod
    def load_kube_config(
        cls,
        config_file: str = None,
        is_in_cluster: bool = None,
        extra_config_locations: List[str] = None,
        context: str = None,
        persist: bool = False,
    ):
        """Loads a kubernetes configuration

        Args:
            config_file (str, optional): The configuration file path. Defaults to None = search for config.
            is_in_cluster (bool, optional): If true, the client will expect to run inside a cluster
                and to load the cluster config. Defaults to None = auto detect.
            extra_config_locations (List[str], optional): Extra locations to search for a configuration. Defaults to None.
            context (str, optional): The context name to run in. Defaults to None = active context.
            persist (bool, optional): If True, config file will be updated when changed (e.g GCP token refresh).
        """
        is_in_cluster = (
            is_in_cluster
            if is_in_cluster is not None
            else os.environ.get(incluster_config.SERVICE_HOST_ENV_NAME, None) is not None
        )

        configuration = kube_config.Configuration()

        if is_in_cluster and config_file is None:
            # load from cluster.
            loader = incluster_config.InClusterConfigLoader(
                incluster_config.SERVICE_TOKEN_FILENAME, incluster_config.SERVICE_CERT_FILENAME
            )
            loader._load_config()

            configuration.host = loader.host
            configuration.ssl_ca_cert = loader.ssl_ca_cert
            configuration.api_key["authorization"] = "bearer " + loader.token
        else:
            if config_file is None:
                config_possible_locations = join_locations_list(
                    extra_config_locations,
                    KUBERNETES_JOB_OPERATOR_DEFAULT_CONFIG_LOCATIONS,
                )
                for loc in config_possible_locations:
                    if os.path.isfile(expanduser(loc)):
                        config_file = loc

            assert config_file is not None, "Kubernetes config file not provided and default config could not be found."

            if config_file is not None:
                kube_config.load_kube_config(
                    config_file=config_file, context=context, client_configuration=configuration, persist_config=persist
                )
            configuration.filepath = config_file

        return configuration

    def get_default_namespace(self):
        """Returns the default namespace for the current config."""
        namespace = None
        try:
            in_cluster_namespace_fpath = os.path.join(KUBERNETES_SERVICE_ACCOUNT_PATH, "namespace")
            if os.path.exists(in_cluster_namespace_fpath):
                with open(in_cluster_namespace_fpath, "r", encoding="utf-8") as nsfile:
                    namespace = nsfile.read()
            elif self.kube_config.filepath is not None:
                (
                    contexts,
                    active_context,
                ) = list_kube_config_contexts(config_file=self.kube_config.filepath)
                namespace = (
                    active_context["context"]["namespace"] if "namespace" in active_context["context"] else "default"
                )
        except Exception as e:
            raise Exception(
                "Could not resolve current namespace, you must provide a namespace or a context file",
                e,
            )
        return namespace

    def stop(self):
        for hndl in list(self._active_queries) + list(self._active_handlers):
            hndl.stop()

    def _create_query_handler(self, queries: List[KubeApiRestQuery]) -> EventHandler:
        assert isinstance(queries, list), "queries Must be a list of queries"
        assert all([isinstance(q, KubeApiRestQuery) for q in queries]), "All queries must be of type KubeApiRestQuery"
        assert len(queries) > 0, "You must at least send one query"

        handler = EventHandler()
        self._active_handlers.add(handler)

        pending = set(queries)

        def remove_from_pending(q):
            pending.remove(q)
            if len(pending) == 0:
                handler.stop_all_streams()

        q: KubeApiRestQuery = None
        for q in queries:
            self._active_queries.add(q)
            q.on(q.query_ended_event_name, lambda query, client: remove_from_pending(q))
            q.pipe(handler)

        return handler

    def _start_execution(self, queries: List[KubeApiRestQuery]):
        for query in queries:
            self._active_queries.add(query)
            query.start(self)

    def async_query(self, queries: List[KubeApiRestQuery]) -> EventHandler:
        if isinstance(queries, KubeApiRestQuery):
            queries = [queries]

        handler = self._create_query_handler(queries)

        self._start_execution(queries)

        return handler

    def stream(
        self,
        queries: List[KubeApiRestQuery],
        event_name: str = None,
        timeout=None,
        process_event_data: Callable = None,
    ):
        if isinstance(queries, KubeApiRestQuery):
            queries = [queries]

        strm = self._create_query_handler(queries).stream(
            event_name, timeout, use_async_loop=False, process_event_data=process_event_data
        )

        self._start_execution(queries)

        for ev in strm:
            yield ev

    def query(
        self,
        queries: List[KubeApiRestQuery],
        event_name: str = KubeApiRestQuery.data_event_name,
        timeout=None,
    ):
        strm = self.stream(queries, event_name=event_name, timeout=timeout)
        return [v for v in strm]
