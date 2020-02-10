import threading
import json
from time import sleep
from queue import SimpleQueue
from .event_handler import EventHandler
from urllib3.connectionpool import MaxRetryError, ReadTimeoutError, TimeoutError
from urllib3.response import HTTPResponse
from kubernetes.stream.ws_client import ApiException

# implements a continues watch stream for a specific
# request.


class ThreadedKubernetesWatchThreadEvent:
    event_type: str = None
    event_value = None

    def __init__(self, event_type, value=None):
        super().__init__()
        self.event_type = event_type
        self.event_value = value


class ThreadedKubernetesWatch(EventHandler):
    _streaming_thread: threading.Thread = None
    _stream_queue: SimpleQueue = None
    _reader_response: HTTPResponse = None
    create_response_stream: callable = None
    read_as_object: bool = True
    _thread_cleanly_exiting = False
    allow_reconnect: bool = True
    reconnect_max_retries: int = 20
    reconnect_wait_timeout: int = 5
    response_wait_timeout: int = 5
    add_default_stream_params: bool = True
    ignore_errors_if_removed: bool = True

    def __init__(self, create_response_stream: callable, read_as_object: bool = True):
        """Creates a kubernetes threaded watcher that allows
        streaming events from kubernetes. The watcher implements a reconnect
        specifications.
        
        Arguments:
            create_response_stream {callable} -- The query create command
        
        Keyword Arguments:
            read_as_object {bool} -- If true, the response value is a dictionary (as json), and should be parsed. 
            (default: {True})
        """
        super().__init__()
        self.create_response_stream = create_response_stream
        self.read_as_object = read_as_object

    def read_event(self, data):
        """Parses the event data.
        
        Arguments:
            data {str} -- The event data.
        
        Returns:
            dict/str -- Returns the parsed data, depending on read_as_object.
        """
        if self.read_as_object:
            return json.loads(data)
        else:
            return data

    def _reader_thread(self, *args, **kwargs):
        """Private - The reader thread.
        
        Yields:
            str -- data line.
        """
        # define a line reader.
        def read_lines():
            prev = ""
            for seg in self._reader_response.read_chunked(decode_content=False):
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

        was_started = False
        reconnect_attempts = 0

        def reconnect_attempt(e: Exception):
            nonlocal reconnect_attempts
            reconnect_attempts += 1
            if reconnect_attempts == self.reconnect_max_retries:
                raise e
            self._stream_queue.put(ThreadedKubernetesWatchThreadEvent("warning", e))
            sleep(self.reconnect_wait_timeout)

        if self.add_default_stream_params:
            # adding required streaming query values.
            kwargs["_preload_content"] = False
            kwargs["_request_timeout"] = self.response_wait_timeout

        # always read until stopped or object dose not exist.
        try:
            while True:
                try:
                    # closing any current response streams.
                    self._close_response_stream(False)

                    self._reader_response = self.create_response_stream(*args, **kwargs)
                    reconnect_attempts = 0
                    was_started = True
                    for line in read_lines():
                        # was deleted externally.
                        if self._stream_queue is None:
                            break

                        kuberentes_event_data = self.read_event(line)
                        self._stream_queue.put(
                            ThreadedKubernetesWatchThreadEvent(
                                "data", kuberentes_event_data
                            )
                        )

                    # was deleted externally.
                    if self._stream_queue is None:
                        break

                except ApiException as e:
                    # exception_body = json.loads(e.body)
                    if e.reason == "Not Found" or e.reason == "Bad Request":
                        if was_started and self.ignore_errors_if_removed:
                            break
                        else:
                            raise e
                except MaxRetryError as e:
                    reconnect_attempt(e)
                except ReadTimeoutError as e:
                    reconnect_attempt(e)
                except TimeoutError as e:
                    reconnect_attempt(e)
                except Exception as e:
                    raise e

        except Exception as e:
            self._stream_queue.put(ThreadedKubernetesWatchThreadEvent("error", e))
        finally:
            self._close_response_stream(False)

        # clean exit.
        self._thread_cleanly_exiting = True
        self.stop()

    def stream(self, *args, **kwargs):
        """Stream the events from the kubernetes cluster, using yield.
        
        NOTE: Any arguments provided to this function will passed to the callable
        method provided in the constructor. (create_response_stream)

        Yields:
            any -- The event object/str
        """
        if self._stream_queue is not None:
            raise Exception("Already streaming, cannot start multiple streams")

        self._thread_cleanly_exiting = False
        self._stream_queue = SimpleQueue()
        self._streaming_thread = threading.Thread(
            target=lambda args, kwargs: self._reader_thread(*args, **kwargs),
            args=(args, kwargs),
        )

        self._streaming_thread.start()

        self.emit("watcher_started")

        try:
            while True:
                event = self._stream_queue.get()
                if not isinstance(event, ThreadedKubernetesWatchThreadEvent):
                    raise Exception(
                        "Invalid queue stream object type. Must be an instance of ThreadedKubernetesWatchThreadEvent"
                    )
                if event.event_type == "data":
                    self.emit("data", event.event_value)
                    yield event.event_value
                elif event.event_type == "stop":
                    break
                elif event.event_type == "warning":
                    self.emit("warning", event.event_value)
                elif event.event_type == "error":
                    # raise the error from the thread.
                    self.emit("error", event.event_value)
                    raise event.event_value
                else:
                    raise Exception(
                        f"Watch event of type {event.event_type} is unknown"
                    )
        except Exception as e:
            raise e
        finally:
            if self._thread_cleanly_exiting:
                if self._streaming_thread.isAlive():
                    self._streaming_thread.join()
            else:
                self._abort_thread()
            self._close_response_stream()
            self._streaming_thread = None
            self._stream_queue = None

        self.emit("watcher_stopped")

    def _abort_thread(self):
        """Call to abort the current running thread.
        """
        if self._streaming_thread is not None:
            if self._streaming_thread.is_alive():
                # FIXME: Find a better way to stop the thread.
                self._streaming_thread._reset_internal_locks(False)
                self._streaming_thread._stop()

    def _close_response_stream(self, throw_errors=True):
        """Stop the current response, if any
        
        Keyword Arguments:
            throw_errors {bool} -- If true, throw errors from the stop process. (default: {True})
        """
        if self._reader_response is not None and not self._reader_response.isclosed():
            try:
                self._reader_response.close()
                self._reader_response.release_conn()
            except Exception as e:
                if throw_errors:
                    raise e
                self.emit("response_error", e)

    def stop(self):
        """Stop the read stream cleanly.
        """
        if self._stream_queue is not None:
            self._stream_queue.put(ThreadedKubernetesWatchThreadEvent("stop"))

    def abort(self):
        """Force abort the response stream and thread.
        """
        self._abort_thread()
        self._close_response_stream(False)
        self._streaming_thread = None
        self._stream_queue = None
