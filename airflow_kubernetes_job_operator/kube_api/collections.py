import re
from enum import Enum
from typing import Callable, List, Dict


def not_empty_string(val: str):
    """Returns true if the string is not empty (len>0) and not None"""
    return isinstance(val, str) and len(val) > 0


class KubeResourceState(Enum):
    """Represents the state of an resource.

    Args:
        Enum ([type]): [description]

    Returns:
        [type]: [description]
    """

    Pending = "Pending"
    Active = "Active"
    Succeeded = "Succeeded"
    Failed = "Failed"
    Running = "Running"
    Deleted = "Deleted"

    def __str__(self) -> str:
        return self.value

    def __repr__(self):
        return str(self)


class KubeApiRestQueryConnectionState(Enum):
    Disconnected = "Disconnected"
    Connecting = "Connecting"
    Streaming = "Streaming"

    def __str__(self) -> str:
        return self.value


def parse_kind_state_default(yaml: dict) -> "KubeResourceState":
    return KubeResourceState.Active


global kinds_collection

kinds_collection = {}


class KubeResourceKind:
    def __init__(
        self,
        name: str,
        api_version: str,
        parse_kind_state: Callable = None,
        auto_include_in_watch: bool = True,
    ):
        """Represents a kubernetes resource kind.

        Args:
            name (str): The kind name (Pod, Job, Service ...)
            api_version (str): The resource api version.
            parse_kind_state (Callable, optional): A method, lambda yaml: object -> KubeResourceState. If exists
            will be used to parse the state of the object. Defaults to None.
            auto_include_in_watch (bool, optional): When a watcher is called, should this object be included.
                Defaults to True.
        """
        super().__init__()

        assert isinstance(name, str) and len(name.strip()) > 0, ValueError("Invalid kind name: " + name)
        assert isinstance(api_version, str) and len(api_version.strip()) > 0, ValueError(
            "Invalid kind api_version: " + api_version
        )
        assert parse_kind_state is None or isinstance(
            parse_kind_state, Callable
        ), "parse_kind_state must be None or a callable"

        self._name = name.lower()
        self.parse_kind_state = parse_kind_state
        self.api_version = api_version
        self.auto_include_in_watch = auto_include_in_watch

    @property
    def name(self) -> str:
        return self._name

    @property
    def plural(self) -> str:
        return self.name + "s"

    def parse_state(self, body: dict, was_deleted: bool = False) -> KubeResourceState:
        """Parses the state of the kind given the object body.

        Args:
            body (dict): Returns the kind state.
            was_deleted (bool, optional): If true, will return KubeResourceState.Deleted. Defaults to False.

        Returns:
            KubeResourceState: The state of the current object.
        """
        if was_deleted:
            return KubeResourceState.Deleted
        else:
            state = (self.parse_kind_state or parse_kind_state_default)(body)
            if not isinstance(state, KubeResourceState):
                state = KubeResourceState(state)
            return state

    def compose_resource_path(
        self,
        namespace: str,
        name: str = None,
        api_version: str = None,
        suffix: str = None,
    ) -> str:
        """Create a resource path from the kind.

        Args:
            namespace (str): The kind namespace to add.
            name (str, optional): The resource name to add. Defaults to None.
            api_version (str, optional): Override the kind api_version. Defaults to None.
            suffix (str, optional): The additional resource suffix (like 'logs'). Defaults to None.

        Returns:
            str: The resource path.
        """
        api_version = api_version or self.api_version
        version_header = "apis"
        if re.match(r"v[0-9]+", api_version):
            version_header = "api"
        composed = [
            version_header,
            api_version,
            "namespaces",
            namespace,
            self.plural,
            name,
            suffix,
        ]
        resource_path = ("/".join([v for v in composed if v is not None])).strip()
        if not resource_path.startswith("/"):
            resource_path = "/" + resource_path
        return resource_path

    @classmethod
    def create_from_existing(
        cls,
        name: str,
        api_version: str = None,
        parse_kind_state: Callable = None,
    ) -> "KubeResourceKind":
        """Create an object kind and fill the default values from the one existing in
        global collection.

        Args:
            name (str): The kind name
            api_version (str, optional): The api version. Defaults to None.
            parse_kind_state (Callable, optional): The method to use to parse the resource state. Defaults to None.

        Returns:
            KubeResourceKind: The new kind
        """
        global kinds_collection
        assert not_empty_string(name), ValueError("name cannot be null")
        name = name.lower()
        if name.lower() not in kinds_collection:
            return KubeResourceKind(name, api_version, parse_kind_state)
        global_kind = cls.get_kind(name)

        return KubeResourceKind(
            name,
            api_version or global_kind.api_version,
            parse_kind_state or global_kind.parse_kind_state,
        )

    @classmethod
    def has_kind(cls, name: str) -> bool:
        global kinds_collection
        return name in kinds_collection

    @classmethod
    def get_kind(cls, name: str) -> "KubeResourceKind":
        global kinds_collection
        assert isinstance(name, str) and len(name) > 0, ValueError("Kind must be a non empty string")
        name = name.lower()
        assert name in kinds_collection, ValueError(
            f"Unknown kubernetes object kind: {name},"
            + " you can use KubeResourceKind.register_global_kind to add new ones."
            + " (airflow_kubernetes_job_operator.kube_api.KubeResourceKind)"
        )
        return kinds_collection[name]

    @classmethod
    def all(cls) -> List["KubeResourceKind"]:
        global kinds_collection
        return list(kinds_collection.values())

    @classmethod
    def parsable(cls) -> List["KubeResourceKind"]:
        """Returns all parsable kinds (i.e. have parse_kind_state not None)"""
        return [k for k in cls.all() if k.parse_kind_state is not None]

    @classmethod
    def watchable(cls) -> List["KubeResourceKind"]:
        """Returns all the kinds that have auto_include_in_watch as true"""
        return [k for k in cls.all() if k.auto_include_in_watch is True]

    @classmethod
    def all_names(cls) -> List[str]:
        global kinds_collection
        return list(kinds_collection.keys())

    @classmethod
    def register_global_kind(cls, kind: "KubeResourceKind"):
        """Add a kind to the global kinds collection"""
        global kinds_collection
        kinds_collection[kind.name] = kind
        return kind

    @staticmethod
    def parse_state_job(yaml: dict) -> KubeResourceState:
        status = yaml.get("status", {})
        conditions = status.get("conditions", [])

        job_status = KubeResourceState.Pending

        if "startTime" in status:
            job_status = KubeResourceState.Running

        condition: dict = None
        for condition in conditions:
            if condition.get("type") == "Failed":
                job_status = KubeResourceState.Failed
            if condition.get("type") == "Complete":
                job_status = KubeResourceState.Succeeded

        return job_status

    @staticmethod
    def _get_container_resource_states_by_name(yaml: dict) -> Dict[str, KubeResourceState]:
        container_statuses = yaml.get("status", {}).get("containerStatuses", [])
        state_by_name: dict = {}
        for container_status in container_statuses:
            name = container_status.get("name", None)
            state = KubeResourceState.Active

            container_state: dict = container_status.get("state", {})

            terminated_info: dict = container_state.get("terminated", None)
            running_info: dict = container_state.get("running", None)
            waiting_info: dict = container_state.get("waiting", None)

            if waiting_info is not None:
                state = KubeResourceState.Pending
            elif running_info is not None:
                state = KubeResourceState.Running
            elif terminated_info is not None:
                if terminated_info.get("errorCode", 0) != 0:
                    state = KubeResourceState.Failed
                else:
                    state = KubeResourceState.Succeeded

            state_by_name[name] = state
        return state_by_name

    @staticmethod
    def parse_state_pod(yaml: dict) -> KubeResourceState:
        """A general method for running pod state. Includes metadata symbols for running
        containers and sidecars.

        Args:
            yaml (dict): The body of the current pod deployment.

        Metadata annotations:
            kubernetes_job_operator.main_container: The name of the main container to watch.

        Returns:
            KubeResourceState: The resource state for the deployment.
        """
        status: dict = yaml.get("status", {})
        annotations: dict = yaml.get("metadata", {}).get("annotations", {})
        main_container_name = annotations.get("kubernetes_job_operator.main_container", None)

        container_resource_states = KubeResourceKind._get_container_resource_states_by_name(yaml=yaml)

        for container_state in container_resource_states.values():
            if container_state == KubeResourceState.Failed:
                return KubeResourceState.Failed

        pod_phase = status.get("phase")
        if pod_phase == "Pending":
            # check for image pull back-off
            container_statuses: List[dict] = status.get("containerStatuses", [])
            for container_status in container_statuses:
                waiting_reason = container_status.get("state", {}).get("waiting", {}).get("reason")
                if waiting_reason == "ImagePullBackOff":
                    return KubeResourceState.Failed

            return KubeResourceState.Pending
        elif pod_phase == "Succeeded":
            return KubeResourceState.Succeeded
        elif pod_phase == "Failed":
            return KubeResourceState.Failed
        elif pod_phase == "Running":
            if main_container_name is not None and main_container_name in container_resource_states:
                return container_resource_states[main_container_name]
            return KubeResourceState.Running

        return pod_phase

    def __eq__(self, o: "KubeResourceKind") -> bool:
        if not isinstance(o, KubeResourceKind):
            return False
        return o.api_version == self.api_version and o.name == self.name

    def __hash__(self) -> int:
        return hash(f"{self.api_version}/{self.name}")

    def __str__(self) -> str:
        return f"{self.api_version}/{self.plural}"


for kind in [
    KubeResourceKind(api_version="v1", name="Pod", parse_kind_state=KubeResourceKind.parse_state_pod),
    KubeResourceKind(api_version="v1", name="Service"),
    KubeResourceKind(api_version="v1", name="Event", auto_include_in_watch=False),
    KubeResourceKind(api_version="batch/v1", name="Job", parse_kind_state=KubeResourceKind.parse_state_job),
    KubeResourceKind(api_version="apps/v1", name="Deployment"),
    KubeResourceKind(api_version="v1", name="ConfigMap"),
    KubeResourceKind(api_version="v1", name="Secret"),
]:
    KubeResourceKind.register_global_kind(kind)


class KubeResourceDescriptor:
    def __init__(
        self,
        body: dict,
        api_version: str = None,
        namespace: str = None,
        name: str = None,
        assert_metadata: bool = True,
    ):
        """A resource descriptor. Parses the body dictionary for values.

        Args:
            body (dict): The resource body.
            api_version (str, optional): The resource api_version_override/fill. Defaults to None.
            namespace (str, optional): The resource namespace override/fill. Defaults to None.
            name (str, optional): The resource name override/fill. Defaults to None.
            assert_metadata (bool, optional): If true, fills the metadata collection if it dose not exist.
                Defaults to True.
        """
        super().__init__()
        assert isinstance(body, dict), ValueError("Error while parsing resource: body must be a dictionary", body)

        self._body = body
        self._kind = (
            None
            if self.body.get("kind", None) is None
            else KubeResourceKind.create_from_existing(
                self.body.get("kind"),
                api_version or self.body.get("apiVersion"),
            )
        )

        if assert_metadata:
            if "metadata" not in self.body:
                self.body["metadata"] = {}

        if namespace or self.namespace:
            self.metadata["namespace"] = namespace or self.namespace
        if name or self.name:
            self.metadata["name"] = name or self.name

    @property
    def self_link(self) -> str:
        return self.metadata["self-link"]

    @property
    def body(self) -> dict:
        return self._body

    @property
    def kind(self) -> KubeResourceKind:
        return self._kind

    @property
    def kind_name(self) -> str:
        if self.kind is None:
            return self.body.get("kind", "{unknown}")
        return self.kind.name

    @property
    def kind_plural(self) -> object:
        return self.kind_name.lower() + "s" if self.kind is not None else None

    @property
    def spec(self) -> dict:
        return self.body.get("spec", {})

    @property
    def status(self) -> dict:
        return self.body.get("status")

    @property
    def state(self) -> KubeResourceState:
        return self.kind.parse_state(self.body, False)

    @property
    def name(self) -> str:
        return self.metadata.get("name", None)

    @name.setter
    def name(self, val: str):
        self.metadata["name"] = val

    @property
    def namespace(self) -> str:
        return self.metadata.get("namespace", None)

    @namespace.setter
    def namespace(self, val: str):
        self.metadata["namespace"] = val

    @property
    def metadata(self) -> dict:
        return self.body.get("metadata", {})

    @property
    def api_version(self) -> str:
        return self.kind.api_version

    def __str__(self):
        if self.namespace is not None:
            if self.name:
                return f"{self.namespace}/{self.kind_plural}/{self.name}"
            else:
                return f"{self.namespace}/{self.kind_plural}/{self.name}"
        else:
            return f"{self.api_version}/{self.kind_name}"
