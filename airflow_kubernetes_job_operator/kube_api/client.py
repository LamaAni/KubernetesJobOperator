# Implements a rest api
from logging import Logger
import json
import traceback
import time

from typing import List, Callable, Set, Union
from weakref import WeakSet
from zthreading.tasks import Task
from zthreading.events import Event, EventHandler

from airflow_kubernetes_job_operator.kube_api.utils import clean_dictionary_nulls

from kubernetes.stream.ws_client import ApiException as KubernetesNativeApiException
from kubernetes.client import ApiClient
from urllib3.response import HTTPResponse
from kubernetes.config import kube_config

from airflow_kubernetes_job_operator.kube_api.utils import kube_logger
from airflow_kubernetes_job_operator.kube_api.exceptions import KubeApiException, KubeApiClientException
from airflow_kubernetes_job_operator.kube_api.collections import KubeApiRestQueryConnectionState
from airflow_kubernetes_job_operator.kube_api.config import (
    KubeApiConfiguration,
    DEFAULT_AUTO_RECONNECT_MAX_ATTEMPTS,
    DEFAULT_AUTO_RECONNECT_WAIT_BETWEEN_ATTEMPTS,
)


def set_asyncio_mode(is_active: bool):
    """NOT_IMPLEMENTED. Sets the global (default) asyncio mode for all queries.
    This method is currently not implemented.

    Args:
        is_active (bool): [description]

    Raises:
        NotImplementedError: [description]
    """
    if is_active:
        raise NotImplementedError("AsyncIO not yet implemented.")
    KubeApiRestQuery.default_use_asyncio = is_active


def get_asyncio_mode() -> bool:
    """Returns the asyncio mode. Defaults to true if not defined in env (reduces memory)"""
    return KubeApiRestQuery.default_use_asyncio


class KubeApiRestQuery(Task):
    data_event_name = "kube_api_query_result"
    default_use_asyncio = False

    query_started_event_name = "kube_api_query_started"
    connection_state_changed_event_name = "kube_api_connection_state"
    query_ended_event_name = "kube_api_query_ended"
    query_before_reconnect_event_name = "kube_api_query_before_reconnect"

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
        auto_reconnect: bool = False,
        auto_reconnect_max_attempts: int = DEFAULT_AUTO_RECONNECT_MAX_ATTEMPTS,
        auto_reconnect_wait_between_attempts: float = DEFAULT_AUTO_RECONNECT_WAIT_BETWEEN_ATTEMPTS,
        throw_on_if_first_api_call_fails: bool = True,
    ):
        """Implements a self containing query task (thread) which handles the retrieval
        of data from the kubernetes api.

        Args:
            resource_path (str): The api resource path. (Without the server, i.e. api/v1/pods)
            method (str, optional): The http method. Defaults to "GET".
            path_params (dict, optional): A dictionary of path parts to send to the server. Defaults to None.
            query_params (dict, optional): A dictionary of query params (the api request params). Defaults to None.
            form_params (list, optional): A dictionary of forms params to send to the server. Defaults to None.
            files (dict, optional): A list of files to send to the server. Defaults to None.
            body (dict, optional): The request body (usual the PUT element). Defaults to None.
            headers (dict, optional): The request headers/override headers. Defaults to None.
            timeout (float, optional): The request timeout. Defaults to None.
            use_asyncio (bool, optional): Not implemented yet! Defaults to None.
            auto_reconnect (bool, optional): If true attempts to auto reconnect if connection is lost.
                Defaults to False.
            auto_reconnect_max_attempts (int, optional): Max number of consecutive auto reconnect requests.
                Defaults to DEFAULT_AUTO_RECONNECT_MAX_ATTEMPTS.
            auto_reconnect_wait_between_attempts (float, optional): Wait time in seconds.
                Defaults to DEFAULT_AUTO_RECONNECT_WAIT_BETWEEN_ATTEMPTS.
            throw_on_if_first_api_call_fails (bool, optional): If true the and the first attempt to connect fails,
                throws an error. Defaults to True.
        """
        assert use_asyncio is not True, NotImplementedError("AsyncIO not yet implemented.")
        super().__init__(
            self._execute_query,
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
        self.auto_reconnect = auto_reconnect
        self.auto_reconnect_max_attempts = auto_reconnect_max_attempts
        self.auto_reconnect_wait_between_attempts = auto_reconnect_wait_between_attempts
        self.throw_on_if_first_api_call_fails = throw_on_if_first_api_call_fails

        # these event are object specific
        self.query_started_event_name = f"{self.query_started_event_name} {id(self)}"
        self.query_ended_event_name = f"{self.query_started_event_name} {id(self)}"
        self.connection_state_changed_event_name = f"{self.connection_state_changed_event_name} {id(self)}"

        self._active_responses: Set[HTTPResponse] = WeakSet()  # type:ignore
        self._is_being_stopped: bool = False
        self._connection_state: KubeApiRestQueryConnectionState = KubeApiRestQueryConnectionState.Disconnected

    @property
    def query_running(self) -> bool:
        """True if the query is executing (connecting or streaming)"""
        return self.connection_state != KubeApiRestQueryConnectionState.Disconnected

    @property
    def connection_state(self) -> KubeApiRestQueryConnectionState:
        """The state of the connection"""
        return self._connection_state

    def _set_connection_state(self, state: KubeApiRestQueryConnectionState, emit_event: bool = True):
        if self._connection_state == state:
            return
        self._connection_state = state
        kube_logger.debug(f"[{self.resource_path}] {self._connection_state}")
        if emit_event:
            self.emit(self.connection_state_changed_event_name, state)

    def _get_method_content_type(self) -> str:
        if self.method == "PATCH":
            return "application/json-patch+json"
        else:
            return "application/json"
        return None

    def parse_data(self, line: str):
        """Overridable, parse the query line data from string to some other
        object.

        Args:
            line (str): The data line.

        Returns:
            any: Parsed object.
        """
        return line

    def emit_data(self, data):
        """Overridable. Called to emit a new data element as event.

        Args:
            data (any): The data element, as returned from parse_data
        """
        self.emit(self.data_event_name, data)

    @classmethod
    def _read_response_stream_lines(cls, response: HTTPResponse):
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
        """Called before the request stars

        Args:
            client (KubeApiRestClient): The client
        """
        pass

    def post_request(self, client: "KubeApiRestClient"):
        """Called after the request ends.

        Args:
            client (KubeApiRestClient): The client.
        """
        pass

    def on_reconnect(self, client: "KubeApiRestClient"):
        """Called before the connection is reestablished.

        Args:
            client (KubeApiRestClient): The client

        Returns:
            False if not to emit the reconnect event.
        """
        return True

    def _execute_query(self, client: "KubeApiRestClient"):
        self._set_connection_state(KubeApiRestQueryConnectionState.Disconnected, False)
        self.emit(self.query_started_event_name, self, client)
        self.pre_request(client)

        try:
            self.query_loop(client)
            self.post_request(client)
        except Exception as ex:
            self.emit_error(ex)
            raise ex
        finally:
            self._set_connection_state(KubeApiRestQueryConnectionState.Disconnected)
            self.emit(self.query_ended_event_name, self, client)

    def query_loop(self, client: "KubeApiRestClient"):
        """Overridable. The main query loop. Called to execute the query.

        Args:
            client (KubeApiRestClient): The client.
        """

        def validate_dictionary(d: dict, default: dict = None):
            if default is not None:
                update_with = d or {}
                d = default.copy()
                d.update(update_with)
            else:
                d = (d or {}).copy()

            return clean_dictionary_nulls(d)

        headers = validate_dictionary(
            self.headers,  # type:ignore
            default={
                "Accept": "application/json",
                "Content-Type": self._get_method_content_type(),
            },
        )

        total_reconnects = 0
        total_consecutive_reconnects = 0

        def can_reconnect():
            # check for change in state
            if not self.auto_reconnect:
                return False

            nonlocal total_consecutive_reconnects
            nonlocal total_reconnects
            total_consecutive_reconnects += 1
            total_reconnects += 1

            if total_consecutive_reconnects >= self.auto_reconnect_max_attempts:
                return False

            return True

        # starting query.
        is_first_connect_attempt = True
        while self.is_running and not self._is_being_stopped and (is_first_connect_attempt or self.auto_reconnect):
            try:
                if not is_first_connect_attempt:
                    # error while running and has wait time
                    if self.query_running and self.auto_reconnect_wait_between_attempts > 0:
                        kube_logger.debug(
                            f"[{self.resource_path}][Reconnect] Sleeping for "
                            + f"{self.auto_reconnect_wait_between_attempts}"
                        )
                        time.sleep(self.auto_reconnect_wait_between_attempts)

                    # check reconnect
                    do_reconnect = self.on_reconnect(client) is not False
                    if do_reconnect:
                        self.emit(self.query_before_reconnect_event_name)

                    # Reset the connection state.
                    self._set_connection_state(KubeApiRestQueryConnectionState.Disconnected)

                    # Case auto_reconnect has changed.
                    if not self.auto_reconnect or not do_reconnect:
                        break

                    kube_logger.debug(f"[{self.resource_path}] Connection lost, reconnecting..")

                # generating the query params
                path_params = validate_dictionary(self.path_params)
                query_params = validate_dictionary(self.query_params)
                form_params = validate_dictionary(self.form_params)  # type:ignore
                files = validate_dictionary(self.files)  # type:ignore

                query_params_array = []
                for k in query_params:
                    query_params_array.append((k, query_params[k]))

                # connecting
                self._set_connection_state(KubeApiRestQueryConnectionState.Connecting)
                request_info = client.api_client.call_api(
                    resource_path=self.resource_path,
                    method=self.method,
                    path_params=path_params,
                    query_params=query_params_array,
                    header_params=headers,
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

                # Connection successful.
                is_first_connect_attempt = False

                response: HTTPResponse = request_info[0]  # type:ignore
                self._active_responses.add(response)
                total_consecutive_reconnects = 0

                # parsing data
                self._set_connection_state(KubeApiRestQueryConnectionState.Streaming)
                for line in self._read_response_stream_lines(response):
                    data = self.parse_data(line)  # type:ignore
                    self.emit_data(data)

                # Proper disconnect.
                self._set_connection_state(KubeApiRestQueryConnectionState.Disconnected)

            except KubernetesNativeApiException as ex:
                # We throw errors in the following cases:
                # 1. Fail to reconnect on first go.
                # 2. Fail while streaming with a non rest api error (otherwise disconnect)
                # 3. Failed too many times.
                if ex.body is not None:
                    # Proper API error should be handled internally and checked for events.
                    try:
                        ex.body = json.loads(ex.body)
                    except Exception:
                        pass

                    exeuctor_name = f"{self.__class__.__module__}.{self.__class__.__name__}"

                    if isinstance(ex.body, dict):
                        exception_message = f"{exeuctor_name}, {ex.reason}: {ex.body.get('message')}"
                    else:
                        exception_message = f"{exeuctor_name}, {ex.reason}: {ex.body}"

                    err = KubeApiClientException(exception_message, rest_api_exception=ex)

                    if is_first_connect_attempt and self.throw_on_if_first_api_call_fails:
                        raise err

                    # check if can reconnect.
                    if can_reconnect():
                        kube_logger.debug(f"[{self.resource_path}] {exception_message}")
                        continue

                    # check if is currently being stopped or already stopped.
                    if self.is_running and not self._is_being_stopped:
                        raise err
                else:
                    raise ex
            except Exception as ex:
                raise ex

    def start(self, client: "KubeApiRestClient"):
        """Start the query execution

        Args:
            client (ApiClient): The api client to use.
        """
        assert not self.is_running, "Cannot start a running query"
        assert isinstance(client, KubeApiRestClient), "client must be of class KubeApiRestClient"

        self._query_running = False
        super().start(client)

        return self

    def wait_until_running(self, timeout: float = 5, ignore_if_running=True):
        """Waits until the query is running. (Thread block)

        Args:
            timeout (float, optional): The wait timeout. Defaults to 5.
            ignore_if_running (bool, optional): If true, do not attempt to wait for the
            start event if query is already running (Streaming or Connecting). Defaults to True.
        """
        if ignore_if_running and self.query_running:
            return

        def predict_is_running(handler, event: Event):
            if event.name != self.connection_state_changed_event_name:
                return False
            if event.args[0] != KubeApiRestQueryConnectionState.Streaming:
                return False
            return True

        self.wait_for(predict_is_running, timeout=timeout)

    def stop(self, timeout: float = None, throw_error_if_not_running: bool = False):
        """Stops the query execution.

        Args:
            timeout (float, optional): Stop timeout (see Task). Defaults to None.
            throw_error_if_not_running (bool, optional): If true, throws an error if not running. Defaults to False.
        """
        try:
            self._is_being_stopped = True
            self.stop_all_streams()
            for rsp in self._active_responses:
                if not rsp.isclosed:
                    try:
                        rsp.close()
                    except Exception:
                        pass
            super().stop(timeout=timeout, throw_error_if_not_running=throw_error_if_not_running)  # type:ignore
        finally:
            self._query_running = False
            self._is_being_stopped = False

    def log_event(self, logger: Logger, ev: Event):
        """Overridable. Called on a log event.

        Args:
            logger (Logger): The logger to log to.
            ev (Event): The log event.
        """
        pass

    def pipe_to_logger(self, logger: Logger = kube_logger, allowed_event_names=None) -> EventHandler:
        """Called to pipe logging events to a specific logger. The log_event method
        will be called when a message is emitted.

        Args:
            logger (Logger, optional): The logger to pipe to. Defaults to kube_logger.
            allowed_event_names (str|List[str]|Enum|list[Enum], optional): The allowed events to log on.
                Defaults to None.

        Returns:
            EventHandler: The handler which is the event pipe.
        """
        allowed_event_names = set(
            allowed_event_names
            or [
                self.data_event_name,
                self.query_before_reconnect_event_name,
            ]
        )

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

        bind_handler = EventHandler(on_event=process_log_event)
        self.pipe(bind_handler)

        return bind_handler


def kube_api_default_stream_process_event_data(ev: Event):
    """Default method for streaming.

    Args:
        ev (Event): The event.

    Returns:
        the first event argument if ev.name == ev.sender.data_event_name else the event.
    """
    if isinstance(ev.sender, KubeApiRestQuery) and ev.name == ev.sender.data_event_name:
        return ev.args[0]
    else:
        return ev


class KubeApiRestClient:
    def __init__(
        self,
        auto_load_kube_config: bool = True,
    ):
        """Creates a new kubernetes api rest client."""
        super().__init__()

        self._active_queries: Set[KubeApiRestQuery] = WeakSet()  # type:ignore
        self._active_handlers: Set[EventHandler] = WeakSet()  # type:ignore
        self.auto_load_kube_config = auto_load_kube_config  # type:ignore
        self._kube_config: kube_config.Configuration = None  # type:ignore
        self._api_client: ApiClient = None  # type:ignore

    @property
    def is_kube_config_loaded(self) -> bool:
        return self._kube_config is not None

    @property
    def kube_config(self) -> kube_config.Configuration:
        """The kubernetes configuration. Use load_kube_config to load

        Returns:
            kube_config.Configuration: The configuration
        """
        if self._kube_config is None:
            if not self.auto_load_kube_config:
                raise KubeApiException("Kubernetes configuration not loaded and auto load is set to false.")
            self.load_kube_config()
            assert self._kube_config is not None, "Failed to load default kubernetes configuration"
        return self._kube_config

    @property
    def api_client(self) -> ApiClient:
        """The underlining rest api client. Will be null if no config has loaded.

        Returns:
            ApiClient: The api client.
        """
        return self._api_client

    def load_kube_config(
        self,
        config_file: str = None,
        context: str = None,
        is_in_cluster: bool = False,
        extra_config_locations: List[str] = None,
        persist: bool = False,
    ):
        """Loads a kubernetes configuration from file.

        Args:
            config_file (str, optional): The configuration file path. Defaults to None = search for config.
            is_in_cluster (bool, optional): If true, the client will expect to run inside a cluster
                and to load the cluster config. Defaults to None = auto detect.
            extra_config_locations (List[str], optional): Extra locations to search for a configuration.
                Defaults to None.
            context (str, optional): The context name to run in. Defaults to None = active context.
            persist (bool, optional): If True, config file will be updated when changed
                (e.g GCP token refresh).
        """
        self._kube_config: kube_config.Configuration = KubeApiConfiguration.load_kubernetes_configuration(
            config_file=config_file,
            is_in_cluster=is_in_cluster,
            extra_config_locations=extra_config_locations,
            context=context,
            persist=persist,
        )

        assert self._kube_config is not None, KubeApiClientException(
            "Failed to load kubernetes configuration. Not found."
        )

        self._api_client: ApiClient = ApiClient(configuration=self.kube_config)

    def get_default_namespace(self) -> str:
        """Returns the default namespace for the current config."""
        assert self.kube_config is not None, KubeApiException(
            "Kubernetes configuration not loaded. use: [client].load_kube_config()"
        )
        return KubeApiConfiguration.get_default_namespace(self.kube_config)

    def stop(self):
        for q in list(self._active_queries):
            q.stop()
        for hndl in list(self._active_handlers):
            hndl.stop_all_streams()

    def _create_query_handler(self, queries: List[KubeApiRestQuery]) -> EventHandler:
        assert isinstance(queries, list), "queries Must be a list of queries"
        assert all([isinstance(q, KubeApiRestQuery) for q in queries]), "All queries must be of type KubeApiRestQuery"
        assert len(queries) > 0, "You must at least send one query"

        handler = EventHandler()
        self._active_handlers.add(handler)

        pending = set(queries)

        def remove_from_pending(q, ex: Exception = None):
            if q in pending:
                pending.remove(q)
            if ex:
                handler.emit_error(ex)
            if len(pending) == 0:
                handler.stop_all_streams()

        for q in queries:
            self._active_queries.add(q)
            q.on(q.error_event_name, lambda query, err: remove_from_pending(query, err))
            q.on(q.query_ended_event_name, lambda query, client: remove_from_pending(query))
            q.pipe(handler)

        return handler

    def _start_execution(self, queries: List[KubeApiRestQuery]):
        for query in queries:
            self._active_queries.add(query)
            query.start(self)

    def query_async(self, queries: Union[List[KubeApiRestQuery], KubeApiRestQuery]) -> EventHandler:
        """Asynchronous querying. The queries will be called in the background. Use wait_until_running
        for each query to wait for the queries to start.

        Args:
            queries (Union[List[KubeApiRestQuery], KubeApiRestQuery]): The queries to execute.

        Returns:
            EventHandler: An event handler where all query events are piped.
        """
        if isinstance(queries, KubeApiRestQuery):
            queries = [queries]

        handler = self._create_query_handler(queries)

        self._start_execution(queries)

        return handler

    def stream(
        self,
        queries: Union[List[KubeApiRestQuery], KubeApiRestQuery],
        event_name: Union[List[str], str] = None,
        timeout=None,
        process_event_data: Callable = kube_api_default_stream_process_event_data,
        throw_errors: bool = True,
    ):
        """Stream events from multiple queries. The queries will be started if they are not.

        Args:
            queries (Union[List[KubeApiRestQuery], KubeApiRestQuery]): The queries to execute/stream.
            event_name (Union[List[str], str], optional): The name of the event to stream. If none will stream
                 query.data_event_name. Defaults to None.
            timeout ([type], optional): The streaming timeout. Errors if the stream has not ended before the timeout
                Defaults to None.
            process_event_data (Callable, optional): post process event data before exiting. Lambda (data)->val
                Defaults to kube_api_default_stream_process_event_data.
            throw_errors (bool, optional): If true throws errors if occur. Defaults to True.

        Yields:
            any: The query result or query events.
        """
        if isinstance(queries, KubeApiRestQuery):
            queries = [queries]

        if event_name is None:
            event_name = list(set([q.data_event_name for q in queries]))

        strm = self._create_query_handler(queries).stream(
            event_name,  # type:ignore
            timeout,
            use_async_loop=False,
            process_event_data=process_event_data,
            throw_errors=throw_errors,
        )

        self._start_execution(queries)

        for ev in strm:
            yield ev

    def query(
        self,
        queries: Union[List[KubeApiRestQuery], KubeApiRestQuery],
        event_name: Union[List[str], str] = None,
        timeout=None,
        throw_errors: bool = True,
    ) -> Union[List[object], object]:
        """Executes the queries and returns the query results. If a single query is sent returns
        a single result. If a list is sent returns a list of query results.

        Args:
            queries (Union[List[KubeApiRestQuery], KubeApiRestQuery]): The queries to execute.
            event_name (Union[List[str], str], optional): The name of the data event,
                if none defaults to query.data_event_name. Defaults to None.
            timeout ([type], optional): The query timeout. Defaults to None.
            throw_errors (bool, optional): If true throw error if they occur. Defaults to True.

        Returns:
            Union[List[object], object]: A single query if a single query is sent. A list if a
                list is sent
        """
        strm = self.stream(queries, event_name=event_name, timeout=timeout, throw_errors=throw_errors)
        rslt = [v for v in strm]
        if not isinstance(queries, list):
            return rslt[0] if len(rslt) > 0 else None
        else:
            return rslt
