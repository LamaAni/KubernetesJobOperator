import yaml
import copy
import logging
import os

from logging import Logger
from uuid import uuid4
from typing import Callable, List, Type, Union
from airflow_kubernetes_job_operator.kube_api.client import KubeApiRestQuery
from airflow_kubernetes_job_operator.utils import randomString
from airflow_kubernetes_job_operator.exceptions import KubernetesJobOperatorException
from airflow_kubernetes_job_operator.kube_api import (
    KubeApiRestClient,
    KubeObjectKind,
    KubeObjectDescriptor,
    NamespaceWatchQuery,
    DeleteNamespaceObject,
    CreateNamespaceObject,
    ConfigureNamespaceObject,
    KubeObjectState,
    GetNamespaceObjects,
    kube_logger,
)


class JobRunner:
    job_runner_instance_id_label_name = "kubernetes-job-runner-instance-id"
    custom_prepare_kinds: dict = {}
    show_runner_id_on_logs: bool = None  # type:ignore

    def __init__(
        self,
        body,
        namespace: str = None,
        random_name_postfix_length=8,
        logger: Logger = kube_logger,
        show_pod_logs: bool = True,
        show_operation_logs: bool = True,
        show_watcher_logs: bool = True,
        show_executor_logs: bool = True,
        show_error_logs: bool = True,
        delete_on_success: bool = True,
        delete_on_failure: bool = False,
        auto_load_kube_config=False,
    ):
        if isinstance(body, str):
            body = yaml.safe_load_all(body)
        if isinstance(body, dict):
            body = [body]

        assert isinstance(body, list) and len(body) > 0 and all(isinstance(o, dict) for o in body), ValueError(
            "body must be a dictionary, a list of dictionaries with at least one value, or string yaml"
        )

        # Make a copy so not to augment the original
        body = copy.deepcopy(body)

        self._client = KubeApiRestClient(auto_load_kube_config=auto_load_kube_config)
        self._body = body
        self.name_postfix = "" if random_name_postfix_length <= 0 else randomString((random_name_postfix_length))
        self.logger: Logger = logger
        self._id = str(uuid4())

        self.namespace = namespace
        self.show_pod_logs = show_pod_logs
        self.show_operation_logs = show_operation_logs
        self.show_watcher_logs = show_watcher_logs
        self.show_executor_logs = show_executor_logs
        self.show_error_logs = show_error_logs
        self.delete_on_success = delete_on_success
        self.delete_on_failure = delete_on_failure

        if self.show_runner_id_on_logs is None:
            self.show_runner_id_on_logs = (
                os.environ.get("KUBERNETES_JOB_OPERATOR_SHOW_RUNNER_ID_IN_LOGS", "false").lower() == "true"
            )

        super().__init__()

    @property
    def id(self) -> str:
        return self._id

    @property
    def body(self) -> List[dict]:
        return self._body

    @property
    def client(self) -> KubeApiRestClient:
        return self._client

    @property
    def job_label_selector(self) -> str:
        return f"{self.job_runner_instance_id_label_name}={self.id}"

    @classmethod
    def register_custom_prepare_kind(cls, kind: Union[KubeObjectKind, str], preapre_kind: Callable):
        if isinstance(kind, str):
            kind = KubeObjectKind.get_kind(kind)

        assert isinstance(kind, KubeObjectKind), ValueError("kind must be an instance of KubeObjectKind or string")
        cls.custom_prepare_kinds[kind.name] = preapre_kind

    @classmethod
    def update_metadata_labels(cls, body: dict, labels: dict):
        assert isinstance(body, dict), ValueError("Body must be a dictionary")
        assert isinstance(labels, dict), ValueError("labels must be a dictionary")

        if isinstance(body.get("spec", None), dict) or isinstance(body.get("metadata", None), dict):
            metadata = body.get("metadata", {})
            if "labels" not in metadata:
                metadata["labels"] = copy.deepcopy(labels)
            else:
                metadata["labels"].update(labels)
        for o in body.values():
            if isinstance(o, dict):
                cls.update_metadata_labels(o, labels)

    @classmethod
    def custom_prepare_job_kind(cls, body: dict):
        assert isinstance(body, dict), ValueError("Body must be a dictionary")
        descriptor = KubeObjectDescriptor(body)

        assert isinstance(descriptor.spec.get("template", None), dict), KubernetesJobOperatorException(
            "Cannot create a job without a template, 'spec.template' is missing or not a dictionary"
        )

        assert isinstance(descriptor.metadata["template"].get("spec", None), dict), KubernetesJobOperatorException(
            "Cannot create a job without a template spec, 'spec.template.spec' is missing or not a dictionary"
        )

        descriptor.spec.setdefault("backoffLimit", 0)
        descriptor.metadata.setdefault("finalizers", [])
        descriptor.spec["template"]["spec"].setdefault("restartPolicy", "Never")

        if "foregroundDeletion" not in descriptor.metadata["finalizers"]:
            descriptor.metadata["finalizers"].append("foregroundDeletion")

    @classmethod
    def custom_prepare_pod_kind(cls, body: dict):
        assert isinstance(body, dict), ValueError("Body must be a dictionary")
        descriptor = KubeObjectDescriptor(body)
        descriptor.spec.setdefault("restartPolicy", "Never")

    def _prepare_kube_object(
        self,
        body: dict,
    ):
        assert isinstance(body, dict), ValueError("Body but be a dictionary")

        kind_name: str = body.get("kind", None)
        if kind_name is None or not KubeObjectKind.has_kind(kind_name.strip().lower()):
            raise KubernetesJobOperatorException(
                f"Unrecognized kubernetes object kind: '{kind_name}', "
                + f"Allowed core kinds are {KubeObjectKind.all_names()}. "
                + "To register new kinds use: KubeObjectKind.register_global_kind(..), "
                + "more information cab be found @ "
                + "https://github.com/LamaAni/KubernetesJobOperator/docs/add_custom_kinds.md",
            )

        descriptor = KubeObjectDescriptor(body)
        assert descriptor.name is not None, ValueError("body['name'] must be defined")
        assert descriptor.spec is not None, ValueError("body['spec'] is not defined")

        descriptor.metadata.setdefault("namespace", self.namespace or self.client.get_default_namespace())
        descriptor.name = f"{descriptor.name}-{self.name_postfix}"

        self.update_metadata_labels(
            body,
            {
                self.job_runner_instance_id_label_name: self.id,
            },
        )

        if kind_name in self.custom_prepare_kinds:
            self.custom_prepare_kinds[kind_name](body)

        return body

    def _create_body_operation_queries(self, operator: Type[ConfigureNamespaceObject]) -> List[KubeApiRestQuery]:
        queries = []
        for obj in self.body:
            q = operator(obj)
            queries.append(q)
            if self.show_operation_logs:
                q.pipe_to_logger(self.logger)
        return queries

    def log(self, *args, level=logging.INFO):
        marker = f"job-runner-{self.id}" if self.show_runner_id_on_logs else "job-runner"
        if self.show_executor_logs:
            self.logger.log(level, f"{{{marker}}}: {args[0] if len(args)>0 else ''}", *args[1:])

    def execute_job(
        self,
        timeout: int = 10,
    ):
        # prepare the run objects.
        namespaces: List[str] = []

        for obj in self.body:
            self._prepare_kube_object(obj)
            namespaces.append(obj["metadata"]["namespace"])

        state_object = KubeObjectDescriptor(self.body[0])

        assert state_object.kind is not None, KubernetesJobOperatorException(
            "The first object in the list of objects must have a recognizable object kind (obj['kind'] is not None)"
        )
        parseable_kinds = [k for k in KubeObjectKind.all() if k.parse_kind_state is not None]
        assert state_object.kind.parse_kind_state is not None, KubernetesJobOperatorException(
            "The first object in the object list must have a kind with a parseable state, "
            + "where the states Failed or Succeeded are returned when the object finishes execution."
            + f"Active kinds with parseable states are: {[k.name for k in parseable_kinds]}. "
            + "To register new kinds or add a parse_kind_state use "
            + "KubeObjectKind.register_global_kind(..) and associated methods. "
            + "more information cab be found @ "
            + "https://github.com/LamaAni/KubernetesJobOperator/docs/add_custom_kinds.md",
        )

        # create the watcher
        watcher = NamespaceWatchQuery(
            namespace=list(set(namespaces)),  # type:ignore
            timeout=timeout,
            watch_pod_logs=self.show_pod_logs,
            label_selector=self.job_label_selector,
        )

        # start the watcher
        if self.show_watcher_logs:
            watcher.pipe_to_logger(self.logger)

        self.client.query_async([watcher])
        watcher.wait_until_running(timeout=timeout)

        self.log(f"Started watcher for kinds: {', '.join(list(watcher.kinds.keys()))}")
        self.log(f"Watching namespaces: {', '.join(namespaces)}")

        # Creating the objects to run
        run_query_handler = self.client.query_async(self._create_body_operation_queries(CreateNamespaceObject))
        # binding the errors.
        run_query_handler.on(run_query_handler.error_event_name, lambda sender, err: watcher.emit_error(err))

        self.log(f"Waiting for state object {state_object.namespace}/{state_object.name} to finish...")
        final_state = watcher.wait_for_state(
            [KubeObjectState.Failed, KubeObjectState.Succeeded],  # type:ignore
            kind=state_object.kind,
            name=state_object.name,
            namespace=state_object.namespace,
            timeout=timeout,
        )

        self.log(f"Job {final_state}")

        if final_state == KubeObjectState.Failed and self.show_error_logs:
            # print the object states.
            kinds = [o.kind.name for o in watcher.watched_objects]
            queries: List[GetNamespaceObjects] = []
            for namespace in namespaces:
                for kind in kinds:
                    queries.append(GetNamespaceObjects(kind, namespace, label_selector=self.job_label_selector))
            self.log("Reading result error (status)..")
            descriptors = [KubeObjectDescriptor(o) for o in self.client.query(queries)]  # type:ignore
            for descriptor in descriptors:
                obj_state: KubeObjectState = KubeObjectState.Active
                if descriptor.kind is not None:
                    obj_state = descriptor.kind.parse_state(descriptor.body)
                if descriptor.status is not None:
                    self.logger.error(f"[{descriptor}]: {obj_state}, status:\n" + yaml.safe_dump(descriptor.status))
                else:
                    self.logger.error(f"[{descriptor}]: Has {obj_state} (status not provided)")

        if (final_state == KubeObjectState.Failed and self.delete_on_failure) or (
            final_state == KubeObjectState.Succeeded and self.delete_on_success
        ):
            self.delete_job()

        self.client.stop()
        self.log("Client stopped")

        return final_state

    def delete_job(self):
        """Using a pre-prepared job yaml, deletes a currently executing job.

        Arguments:

            job_yaml {dict} -- The job description yaml.
        """
        descriptors: List[KubeObjectDescriptor] = [KubeObjectDescriptor(o) for o in self.body]
        descriptors = [d for d in descriptors if d.kind is not None and d.name is not None and d.namespace is not None]

        self.log(
            "Deleting job deployments for items:\n" + "\n".join([f"{d}" for d in descriptors]), level=logging.DEBUG
        )
        self.client.query(self._create_body_operation_queries(DeleteNamespaceObject))
        self.log("Job deleted")


JobRunner.register_custom_prepare_kind("pod", JobRunner.custom_prepare_pod_kind)
JobRunner.register_custom_prepare_kind("job", JobRunner.custom_prepare_job_kind)
