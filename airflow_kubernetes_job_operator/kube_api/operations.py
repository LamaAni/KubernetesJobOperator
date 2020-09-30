from logging import Logger
from typing import Union
from zthreading.events import Event

from airflow_kubernetes_job_operator.kube_api.collections import KubeResourceDescriptor, KubeResourceKind
from airflow_kubernetes_job_operator.kube_api.client import KubeApiRestQuery


class ConfigureNamespaceResource(KubeApiRestQuery):
    def __init__(
        self,
        body: dict,
        method: str = "POST",
        name: str = None,
        namespace: str = None,
        api_version: str = None,
        kind: Union[str, KubeResourceKind] = None,
        use_asyncio=None,
        query_params: dict = None,
        include_body_in_query: bool = True,
    ):
        """General query to change the a kubernetes resource.

        Args:
            body (dict): The resource body (to update/create/delete)
            method (str, optional): The query method. Defaults to "POST".
            name (str, optional): The resource name (override). Defaults to None.
            namespace (str, optional): The resource namespace (override). Defaults to None.
            api_version (str, optional): The resource api_version (override). Defaults to None.
            kind (Union[str, KubeResourceKind], optional): The resource kind (override). Defaults to None.
            use_asyncio ([type], optional): NOT IMPLEMENTED. Defaults to None.
            query_params (dict, optional): The create/delete/patch query params. Defaults to None.
            include_body_in_query (bool, optional): Add the resource body to the query. Defaults to True.
        """
        self._descriptor = KubeResourceDescriptor(body, namespace=namespace, name=name)
        assert self._descriptor.namespace is not None, ValueError(
            "namespace cannot be none, you must send a name in variable or in body"
        )
        assert self._descriptor.api_version is not None, ValueError(
            "The object kind must be defined. body['apiVersion'] != None"
        )

        resource_path = (
            self._descriptor.kind.compose_resource_path(self._descriptor.namespace)
            if method == "POST"
            else self._descriptor.kind.compose_resource_path(self._descriptor.namespace, self._descriptor.name)
        )

        super().__init__(
            resource_path=resource_path,
            query_params=query_params,
            method=method,
            body=body if include_body_in_query else None,
            use_asyncio=use_asyncio,
        )

        self.data_event_name = "configured"

    def log_event(self, logger: Logger, ev: Event):
        """Logging override"""
        if ev.name == self.data_event_name:
            logger.info(f"[{self._descriptor}] {self.data_event_name}")
        return super().log_event(logger, ev)


class CreateNamespaceResource(ConfigureNamespaceResource):
    def __init__(
        self,
        body: dict,
        name: str = None,
        namespace: str = None,
        api_version: str = None,
        kind: Union[str, KubeResourceKind] = None,
        use_asyncio=None,
    ):
        """Creates a resource in a namespace.

        Args:
            body (dict): The resource body
            name (str, optional): The resource name (override). Defaults to None.
            namespace (str, optional): The resource namespace (override). Defaults to None.
            api_version (str, optional): The resource api_version (override). Defaults to None.
            kind (Union[str, KubeResourceKind], optional): The resource kind (override). Defaults to None.
            use_asyncio ([type], optional): NOT IMPLEMENTED. Defaults to None.
        """
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


class DeleteNamespaceResource(ConfigureNamespaceResource):
    def __init__(
        self,
        body,
        name: str = None,
        namespace: str = None,
        api_version: str = None,
        kind: Union[str, KubeResourceKind] = None,
        grace_period_seconds=60,
        propagation_policy=None,
        use_asyncio=None,
    ):
        """Deletes a resource in a namespace

        Args:
            body (dict): The resource body
            name (str, optional): The resource name (override). Defaults to None.
            namespace (str, optional): The resource namespace (override). Defaults to None.
            api_version (str, optional): The resource api_version (override). Defaults to None.
            kind (Union[str, KubeResourceKind], optional): The resource kind (override). Defaults to None.
            grace_period_seconds (int, optional): The grace period before force deleting the resource. Defaults to 60.
            propagation_policy ([type], optional): Delete propagation policy. See kubernetes. Defaults to None.
            use_asyncio ([type], optional): NOT IMPLEMENTED. Defaults to None.
        """
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
