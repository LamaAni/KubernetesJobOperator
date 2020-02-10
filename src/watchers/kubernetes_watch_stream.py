import kubernetes
from urllib3.response import HTTPResponse
from .event_handler import EventHandler

# FIXME: Deprecate this watch stream and move to a more stable
# threaded approach with queue. The queue should allow for
# the process to be re-started and killed properly and
# for the thread to keep seeking the source if connection was lost.


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
        self.emit("started")
        while True:
            try:
                resp = func(*args, **kwargs)
            except Exception as e:
                if not hasattr(e, "reason"):
                    raise e
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
