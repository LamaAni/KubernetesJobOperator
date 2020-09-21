import kubernetes
from logging import Logger
from zthreading.events import Event

from airflow_kubernetes_job_operator.kube_api.collections import KubeObjectKind, KubeObjectDescriptor
from airflow_kubernetes_job_operator.kube_api.exceptions import KubeApiException
from airflow_kubernetes_job_operator.kube_api.client import KubeApiRestQuery, KubeApiRestClient


class ConfigureNamespaceObject(KubeApiRestQuery):
    def __init__(
        self,
        body,
        method: str,
        name: str = None,
        namespace: str = None,
        api_version: str = None,
        kind: str = None,
        use_asyncio=None,
        query_params: dict = None,
        include_body_in_query: bool = True,
    ):
        self._descriptor = KubeObjectDescriptor(body, namespace=namespace, name=name)
        assert self._descriptor.namespace is not None, ValueError(
            "namespace cannot be none, you must send a name in variable or in body"
        )
        assert self._descriptor.api_version is not None, ValueError(
            "The object kind must be defined. body['apiVersion'] != None"
        )

        super().__init__(
            self._descriptor.kind.compose_resource_path(self._descriptor.namespace),
            path_params={
                namespace: self._descriptor.namespace,
                name: self._descriptor.name,
            },
            query_params=query_params,
            method=method,
            body=body if include_body_in_query else None,
            use_asyncio=use_asyncio,
        )

        self.data_event_name = "configured"

    def log_event(self, logger: Logger, ev: Event):
        if ev.name == self.data_event_name:
            logger.info(f"{self._descriptor}: {self.data_event_name}")
        return super().log_event(logger, ev)


class CreateNamespaceObject(ConfigureNamespaceObject):
    def __init__(
        self,
        body,
        name=None,
        namespace=None,
        api_version=None,
        kind=None,
        use_asyncio=None,
    ):
        super().__init__(
            body,
            "POST",
            name=name,
            namespace=namespace,
            api_version=api_version,
            kind=kind,
            use_asyncio=use_asyncio,
        )

        self.data_event_name = "created"


class DeleteNamespaceObject(ConfigureNamespaceObject):
    def __init__(
        self,
        body,
        name=None,
        namespace=None,
        api_version=None,
        kind=None,
        grace_period_seconds=60,
        propagation_policy=None,
        use_asyncio=None,
    ):
        assert grace_period_seconds is not None, ValueError("Grace period seconds must be defined.")
        super().__init__(
            body,
            "DELETE",
            name=name,
            namespace=namespace,
            api_version=api_version,
            kind=kind,
            use_asyncio=use_asyncio,
            query_params={
                "gracePeriodSeconds": grace_period_seconds,
                "propagationPolicy": propagation_policy,
            },
            include_body_in_query=False,
        )

        self.data_event_name = "deleted"
