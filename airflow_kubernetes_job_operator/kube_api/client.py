# Implements a rest api
import os
import json
import yaml
from typing import List, Callable, Set
from weakref import WeakSet
from os.path import expanduser
from zthreading.tasks import Task
from zthreading.events import EventHandler
from requests import request, Response

from airflow_kubernetes_job_operator.kube_api.exceptions import KubeApiException
from airflow_kubernetes_job_operator.kube_api.utils import unqiue_with_order, clean_dictionary_nulls

from kubernetes.client import ApiClient
from kubernetes.config import kube_config, incluster_config, list_kube_config_contexts


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
    os.environ[KUBERNETES_API_CLIENT_USE_ASYNCIO_ENV_NAME] = str(is_active).lower()


def get_asyncio_mode() -> bool:
    """Returns the asyncio mode. Defaults to true if not defined in env (reduces memory)
    """
    return os.environ.get(KUBERNETES_API_CLIENT_USE_ASYNCIO_ENV_NAME, "true") == "true"


class KubeApiRestQuery(Task):
    data_event_name = "kube_api_query_result"
    complete_event_name = "kube_api_query_complete"

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
        use_asyncio: bool = get_asyncio_mode(),
    ):
        super().__init__(
            self._query_loop,
            use_async_loop=use_asyncio,
            use_daemon_thread=True,
            thread_name=f"{self.__class__.__name__} {id(self)}",
            event_name=KubeApiRestQuery.complete_event_name,
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

    def _query_loop(self, client: "KubeApiRestClient"):
        self.pre_request(client)

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
            _return_http_data_only=False,  # noqa: E501
            _preload_content=False,
            _request_timeout=self.timeout,
            collection_formats={},
        )

        response = request_info[0]

        for line in self.read_response_stream_lines(response):
            data = self.parse_data(line)
            self.emit_data(data)

        self.post_request(client)
        self.emit(self.complete_event_name)

    def start(self, client: "KubeApiRestClient"):
        """Start the query execution

        Args:
            client (ApiClient): The api client to use.
        """
        assert not self.is_running, "Cannot start a running query"
        assert isinstance(client, KubeApiRestClient), "client must be of class KubeApiRestClient"
        return super().start(client)


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
            q.on(q.complete_event_name, lambda: remove_from_pending(q))
            q.pipe(handler)

        return handler

    def async_query(self, queries: List[KubeApiRestQuery]) -> EventHandler:
        if isinstance(queries, KubeApiRestQuery):
            queries = [queries]

        handler = self._create_query_handler(queries)

        for q in queries:
            q.start(self)

        return handler

    def stream(self, queries: List[KubeApiRestQuery], event_name: str = KubeApiRestQuery.data_event_name, timeout=None):
        if isinstance(queries, KubeApiRestQuery):
            queries = [queries]

        strm = self._create_query_handler(queries).stream(
            event_name, timeout, use_async_loop=False, process_event_data=lambda ev: ev.args[0]
        )

        for q in queries:
            q.start(self)

        return strm

    def query(self, queries: List[KubeApiRestQuery], event_name: str = KubeApiRestQuery.data_event_name, timeout=None):
        strm = self.stream(queries)
        return [v for v in strm]
