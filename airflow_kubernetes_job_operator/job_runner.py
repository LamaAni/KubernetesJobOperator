import yaml
import copy
import logging
import os

from logging import Logger
from uuid import uuid4
from typing import Callable, List, Type, Union
from airflow_kubernetes_job_operator.kube_api.utils import not_empty_string
from airflow_kubernetes_job_operator.utils import random_string
from airflow_kubernetes_job_operator.collections import JobRunnerDeletePolicy, JobRunnerException
from airflow_kubernetes_job_operator.kube_api import (
    KubeApiConfiguration,
    GetAPIVersions,
    KubeApiRestQuery,
    KubeApiRestClient,
    KubeResourceKind,
    KubeResourceDescriptor,
    NamespaceWatchQuery,
    DeleteNamespaceResource,
    CreateNamespaceResource,
    ConfigureNamespaceResource,
    KubeResourceState,
    GetNamespaceResources,
    kube_logger,
)


class JobRunner:
    job_runner_instance_id_label_name = "kubernetes-job-runner-instance-id"
    custom_prepare_kinds: dict = {}
    show_runner_id_on_logs: bool = False

    def __init__(
        self,
        body: Union[str, dict, List[dict]],
        namespace: str = None,
        logger: Logger = kube_logger,
        show_pod_logs: bool = True,
        show_operation_logs: bool = True,
        show_watcher_logs: bool = True,
        show_executor_logs: bool = True,
        show_error_logs: bool = True,
        delete_policy: JobRunnerDeletePolicy = JobRunnerDeletePolicy.IfSucceeded,
        auto_load_kube_config=True,
        random_name_postfix_length: int = 8,
        name_prefix: str = None,
        name_postfix: str = None,
    ):
        """A kubernetes job runner. Creates a collection of resources and waits for the
        first resource to reach one of KubeResourceState.Succeeded/KubeResourceState.Failed/KubeResourceState.Deleted

        Args:
            body (Union[str, dict, List[dict]]): The execution body (kubernetes definition). If string then yaml is
                expected.
            namespace (str, optional): The default namespace to execute in. Defaults to None.
            logger (Logger, optional): The logger to write to. Defaults to kube_logger.
            show_pod_logs (bool, optional): If true, follow all pod logs. Defaults to True.
            show_operation_logs (bool, optional): If true shows the operation logs (runner). Defaults to True.
            show_watcher_logs (bool, optional): If true, shows the watcher events. Defaults to True.
            show_executor_logs (bool, optional): If true, shows the create event events. Defaults to True.
            show_error_logs (bool, optional): If true, shows errors in the log. Defaults to True.
            delete_policy (JobRunnerDeletePolicy, optional): Determines when to delete the job resources upon finishing
            . Defaults to JobRunnerDeletePolicy.IfSucceeded.
            auto_load_kube_config (bool, optional): If true, and kube_config is None, load it. Defaults to True.
            random_name_postfix_length (int, optional): Add a random string to all resource names if > 0. Defaults to 8.
            name_prefix (str, optional): The prefix for the all resource names. Defaults to None.
            name_postfix (str, optional): The postfix for all resource name. Defaults to None.
        """

        body = body if not isinstance(body, str) else list(yaml.safe_load_all(body))
        body = body if isinstance(body, list) else [body]

        assert all(isinstance(r, dict) for r in body), ValueError(
            "body must be a dictionary, a list of dictionaries with at least one value, or string yaml"
        )

        self._client = KubeApiRestClient(auto_load_kube_config=auto_load_kube_config)
        self._is_body_ready = False
        self._body = body
        self.logger: Logger = logger
        self._id = str(uuid4())

        self.name_postfix = (
            name_postfix or "" if random_name_postfix_length <= 0 else random_string((random_name_postfix_length))
        )
        self.name_prefix = name_prefix

        self.namespace = namespace
        self.show_pod_logs = show_pod_logs
        self.show_operation_logs = show_operation_logs
        self.show_watcher_logs = show_watcher_logs
        self.show_executor_logs = show_executor_logs
        self.show_error_logs = show_error_logs
        self.delete_policy = delete_policy

        if self.show_runner_id_on_logs is None:
            self.show_runner_id_on_logs = SHOW_RUNNER_ID_IN_LOGS

        super().__init__()

    @property
    def id(self) -> str:
        return self._id

    @property
    def body(self) -> List[dict]:
        if not self._is_body_ready:
            self.prepare_body()
        return self._body

    @property
    def client(self) -> KubeApiRestClient:
        return self._client

    @property
    def job_label_selector(self) -> str:
        return f"{self.job_runner_instance_id_label_name}={self.id}"

    @classmethod
    def register_custom_prepare_kind(cls, kind: Union[KubeResourceKind, str], preapre_kind: Callable):
        """Register a global kind preparation method. Runs before execution on a copy.

        Args:
            kind (Union[KubeResourceKind, str]): The object kind.
            preapre_kind (Callable): The preparation method. lambda(runner=self, resource)
        """
        if isinstance(kind, str):
            kind = KubeResourceKind.get_kind(kind)

        assert isinstance(kind, KubeResourceKind), ValueError("kind must be an instance of KubeResourceKind or string")
        cls.custom_prepare_kinds[kind.name] = preapre_kind

    @classmethod
    def _update_metadata_labels(cls, body: dict, labels: dict):
        assert isinstance(body, dict), ValueError("Body must be a dictionary")
        assert isinstance(labels, dict), ValueError("labels must be a dictionary")

        if isinstance(body.get("spec", None), dict) or isinstance(body.get("metadata", None), dict):
            body.setdefault("metadata", {})
            metadata = body["metadata"]
            if "labels" not in metadata:
                metadata["labels"] = copy.deepcopy(labels)
            else:
                metadata["labels"].update(labels)
        for o in body.values():
            if isinstance(o, dict):
                cls._update_metadata_labels(o, labels)

    @classmethod
    def custom_prepare_job_kind(cls, body: dict):
        assert isinstance(body, dict), ValueError("Body must be a dictionary")
        descriptor = KubeResourceDescriptor(body)

        assert isinstance(descriptor.spec.get("template", None), dict), JobRunnerException(
            "Cannot create a job without a template, 'spec.template' is missing or not a dictionary"
        )

        assert isinstance(descriptor.metadata["template"].get("spec", None), dict), JobRunnerException(
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
        descriptor = KubeResourceDescriptor(body)
        descriptor.spec.setdefault("restartPolicy", "Never")

    def prepare_body(self, force=False):
        """Called to prepare all the resources in the body for execution

        Args:
            force (bool, optional): The body preparation will only happen once, unless forced. Defaults to False.
        """
        if not force and self._is_body_ready:
            return

        body = self._body
        if isinstance(body, str):
            body = list(yaml.safe_load_all(body))
        else:
            body = copy.deepcopy(body)
            if not isinstance(body, list):
                body = [body]

        assert all(isinstance(o, dict) for o in body), JobRunnerException(
            "Failed to parse body, found errored collection values"
        )

        for obj in body:
            self._prepare_kube_object(obj)
        self._body = body
        self._is_body_ready = True

    def _prepare_kube_object(
        self,
        body: dict,
    ):
        assert isinstance(body, dict), ValueError("Body but be a dictionary")

        kind_name: str = body.get("kind", None)
        if kind_name is None or not KubeResourceKind.has_kind(kind_name.strip().lower()):
            raise JobRunnerException(
                f"Unrecognized kubernetes object kind: '{kind_name}', "
                + f"Allowed core kinds are {KubeResourceKind.all_names()}. "
                + "To register new kinds use: KubeResourceKind.register_global_kind(..), "
                + "more information cab be found @ "
                + "https://github.com/LamaAni/KubernetesJobOperator/docs/add_custom_kinds.md",
            )

        descriptor = KubeResourceDescriptor(body)
        assert descriptor.spec is not None, ValueError("body['spec'] is not defined")

        descriptor.metadata.setdefault("namespace", self.namespace or self.client.get_default_namespace())
        name_parts = [v for v in [self.name_prefix, descriptor.name, self.name_postfix] if not_empty_string(v)]
        assert len(name_parts) > 0, JobRunnerException("Invalid name or auto generated name")
        descriptor.name = "-".join(name_parts)

        self._update_metadata_labels(
            body,
            {
                self.job_runner_instance_id_label_name: self.id,
            },
        )

        if kind_name in self.custom_prepare_kinds:
            self.custom_prepare_kinds[kind_name](body)

        return body

    def _create_body_operation_queries(self, operator: Type[ConfigureNamespaceResource]) -> List[KubeApiRestQuery]:
        queries = []
        for obj in self.body:
            q = operator(obj)
            queries.append(q)
            if self.show_operation_logs:
                q.pipe_to_logger(self.logger)
        return queries

    def log(self, *args, level=logging.INFO):
        """Log in the runner logger.

        Args:
            level ([type], optional): The log level. Defaults to logging.INFO.
        """
        marker = f"job-runner-{self.id}" if self.show_runner_id_on_logs else "job-runner"
        if self.show_executor_logs:
            self.logger.log(level, f"{{{marker}}}: {args[0] if len(args)>0 else ''}", *args[1:])

    def execute_job(
        self,
        timeout: int = 60 * 5,
        watcher_start_timeout: int = 10,
    ):
        """Execute the job

        Args:
            timeout (int, optional): Execution timeout. Defaults to 60*5.
            watcher_start_timeout (int, optional): The timeout to start watching the namespaces. Defaults to 10.

        Returns:
            KubeResourceState: The main resource (resources[0]) final state.
        """

        self.prepare_body()

        # prepare the run objects.
        namespaces: List[str] = []
        all_kinds: List[KubeResourceKind] = list(KubeResourceKind.watchable())
        descriptors = [KubeResourceDescriptor(r) for r in self.body]

        assert len(descriptors) > 0, JobRunnerException("You must have at least one resource to execute.")

        state_object = descriptors[0]

        assert all(d.kind is not None for d in descriptors), JobRunnerException(
            "All resources in execution must have a recognizable object kind: (resource['kind'] is not None)"
        )

        assert state_object.kind.parse_kind_state is not None, JobRunnerException(
            "The first object in the object list must have a kind with a parseable state, "
            + "where the states Failed or Succeeded are returned when the object finishes execution."
            + f"Active kinds with parseable states are: {[k.name for k in KubeResourceKind.parseable()]}. "
            + "To register new kinds or add a parse_kind_state use "
            + "KubeResourceKind.register_global_kind(..) and associated methods. "
            + "more information cab be found @ "
            + "https://github.com/LamaAni/KubernetesJobOperator/docs/add_custom_kinds.md",
        )

        # scan the api and get all current watchable kinds.
        watchable_kinds = GetAPIVersions.get_existing_api_kinds(self.client, all_kinds)

        for resource in descriptors:
            assert resource.kind is not None, JobRunnerException("Cannot execute an object without a kind")
            assert resource.kind in watchable_kinds, JobRunnerException(
                "All resources specified in the execution (the body) must exist in the api. "
                + f"The kind {str(state_object.kind)} was not found."
            )
            all_kinds.append(resource.kind)
            if resource.namespace is not None:
                namespaces.append(resource.namespace)

        namespaces = list(set(namespaces))
        all_kinds = list(set(all_kinds))

        for kind in KubeResourceKind.watchable():
            if kind not in watchable_kinds:
                self.log(
                    f"Could not find kind '{kind}' in the api server. "
                    + "This kind is not watched and events will not be logged",
                    level=logging.WARNING,
                )

        context_info = KubeApiConfiguration.get_active_context_info(self.client.kube_config)
        self.log(f"Executing context: {context_info.get('name','unknown')}")
        self.log(f"Executing cluster: {context_info.get('context',{}).get('cluster')}")

        # create the watcher
        watcher = NamespaceWatchQuery(
            kinds=watchable_kinds,
            namespace=list(set(namespaces)),  # type:ignore
            timeout=timeout,
            watch_pod_logs=self.show_pod_logs,
            label_selector=self.job_label_selector,
        )

        # start the watcher
        if self.show_watcher_logs:
            watcher.pipe_to_logger(self.logger)

        self.client.query_async([watcher])
        try:
            watcher.wait_until_running(timeout=watcher_start_timeout)
        except TimeoutError as ex:
            raise JobRunnerException(
                "Failed while waiting for watcher to start. Unable to connect to cluster.",
                ex,
            )

        self.log(f"Started watcher for kinds: {', '.join(list(watcher.kinds.keys()))}")
        self.log(f"Watching namespaces: {', '.join(namespaces)}")

        # Creating the objects to run
        run_query_handler = self.client.query_async(self._create_body_operation_queries(CreateNamespaceResource))
        # binding the errors.
        run_query_handler.on(run_query_handler.error_event_name, lambda sender, err: watcher.emit_error(err))

        self.log(f"Waiting for {state_object.namespace}/{state_object.name} to finish...")

        try:
            final_state: KubeResourceState = watcher.wait_for_state(
                [KubeResourceState.Failed, KubeResourceState.Succeeded, KubeResourceState.Deleted],  # type:ignore
                kind=state_object.kind,
                name=state_object.name,
                namespace=state_object.namespace,
                timeout=timeout,
            )
        except Exception as ex:
            self.log("Execution timeout... deleting resources", level=logging.ERROR)
            self.abort()
            raise ex

        if final_state == KubeResourceState.Deleted:
            self.log(f"Failed to execute. Main resource {state_object} was delete", level=logging.ERROR)
            self.abort()
            raise JobRunnerException("Resource was deleted while execution was running, execution failed.")

        self.log(f"Job {final_state}")

        if final_state == KubeResourceState.Failed and self.show_error_logs:
            # print the object states.
            kinds = [o.kind.name for o in watcher.watched_objects]
            queries: List[GetNamespaceResources] = []
            for namespace in namespaces:
                for kind in set(kinds):
                    queries.append(GetNamespaceResources(kind, namespace, label_selector=self.job_label_selector))
            self.log("Reading result error (status) objects..")
            resources = [KubeResourceDescriptor(o) for o in self.client.query(queries)]
            self.log(
                f"Found {len(resources)} resources related to this run:\n"
                + "\n".join(f" - {str(r)}" for r in resources)
            )
            for resource in resources:
                obj_state: KubeResourceState = KubeResourceState.Active
                if resource.kind is not None:
                    obj_state = resource.kind.parse_state(resource.body)
                if resource.status is not None:
                    self.logger.error(f"[{resource}]: {obj_state}, status:\n" + yaml.safe_dump(resource.status))
                else:
                    self.logger.error(f"[{resource}]: Has {obj_state} (status not provided)")

        if (
            self.delete_policy == JobRunnerDeletePolicy.Always
            or (self.delete_policy == JobRunnerDeletePolicy.IfFailed and final_state == KubeResourceState.Failed)
            or (self.delete_policy == JobRunnerDeletePolicy.IfSucceeded and final_state == KubeResourceState.Succeeded)
        ):
            self.log(f"Deleting resources due to policy: {str(self.delete_policy)}")
            self.delete_job()

        self.client.stop()
        self.log("Client stopped, execution completed.")

        return final_state

    def delete_job(self):
        """Using a pre-prepared job yaml, deletes a currently executing job.

        Arguments:

            body {dict} -- The job description yaml.
        """
        self.log(("Deleting job.."))
        descriptors: List[KubeResourceDescriptor] = [KubeResourceDescriptor(o) for o in self.body]
        descriptors = [d for d in descriptors if d.kind is not None and d.name is not None and d.namespace is not None]

        self.log(
            "Deleting objects: " + ", ".join([f"{d}" for d in descriptors]),
        )
        self.client.query(self._create_body_operation_queries(DeleteNamespaceResource))
        self.log("Job deleted")

    def abort(self):
        """Abort the job (and delete all resources)"""
        self.log("Aborting job ...")
        self.delete_job()
        self.client.stop()
        self.log("Client stopped, execution aborted.")


JobRunner.register_custom_prepare_kind("pod", JobRunner.custom_prepare_pod_kind)
JobRunner.register_custom_prepare_kind("job", JobRunner.custom_prepare_job_kind)
