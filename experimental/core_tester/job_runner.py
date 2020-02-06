import kubernetes
import os
import textwrap
import yaml
from time import sleep
from typing import List
import logging
from datetime import datetime

logging.basicConfig(level="INFO")

CUR_DIRECTORY = os.path.abspath(os.path.dirname(__file__))


class LogWatch(kubernetes.watch.Watch):
    @staticmethod
    def iter_resp_lines(resp):
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

        Example:
            v1 = kubernetes.client.CoreV1Api()
            watch = kubernetes.watch.Watch()
            for e in watch.stream(v1.list_namespace, resource_version=1127):
                type = e['type']
                object = e['object']  # object is one of type return_type
                raw_object = e['raw_object']  # raw_object is a dict
                ...
                if should_stop:
                    watch.stop()
        """

        self._stop = False
        return_type = self.get_return_type(func)
        kwargs["watch"] = True
        kwargs["_preload_content"] = False
        if "resource_version" in kwargs:
            self.resource_version = kwargs["resource_version"]

        timeouts = "timeout_seconds" in kwargs
        while True:
            resp = func(*args, **kwargs)
            try:
                for line in LogWatch.iter_resp_lines(resp):
                    yield line
                    if self._stop:
                        break
            finally:
                kwargs["resource_version"] = self.resource_version
                resp.close()
                resp.release_conn()

            if timeouts or self._stop:
                break


def load_raw_formatted_file(fpath):
    text = ""
    with open(fpath, "r", encoding="utf-8") as src:
        text = src.read()
    return text


def create_pod_v1_object(pod_name, pod_image, command) -> kubernetes.client.V1Pod:
    pod_yaml = load_raw_formatted_file(os.path.join(CUR_DIRECTORY, "pod.yaml"))
    pod_yaml = pod_yaml.format(pod_name="lama", pod_image="ubuntu")
    pod = yaml.safe_load(pod_yaml)
    pod["spec"]["containers"][0]["command"] = command
    return pod


# test kubernetes
kubernetes.config.load_kube_config()
contexts, active_context = kubernetes.config.list_kube_config_contexts()
current_namespace = active_context["context"]["namespace"]
client = kubernetes.client.CoreV1Api()
watcher = LogWatch()

bash_script = """
echo "Starting"
while true; do
    date
    sleep 1
done
"""

pod_to_execute = create_pod_v1_object("lama", "ubuntu", ["bash", "-c", bash_script])

print("Validate the pod...")
try:
    status = client.read_namespaced_pod_status("lama", current_namespace)
except:
    print("Pod dose not exist creating...")
    client.create_namespaced_pod(current_namespace, pod_to_execute)
    sleep(10)

print("Watch...")


def read_log(*args, **kwargs):
    val = client.read_namespaced_pod_log_with_http_info(
        "lama", current_namespace, _preload_content=False, follow=True
    )
    return val[0]


started = datetime.now()
for log_line in watcher.stream(read_log):
    logging.info(log_line)
    running_for = datetime.now() - started
    if running_for.seconds > 30:
        watcher.stop()


# for event in watcher.stream(read_log):
#     logging.log(event)

kubernetes.utils.create_from_yaml
