import re
from enum import Enum
from typing import Callable, Dict, List
from airflow_kubernetes_job_operator.kube_api.utils import not_empty_string


class KubeObjectState(Enum):
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


def parse_kind_state_default(yaml: dict) -> "KubeObjectState":
    return KubeObjectState.Active


global kinds_collection
kinds_collection: Dict[str, "KubeObjectKind"] = {}


class KubeObjectKind:
    def __init__(
        self,
        name: str,
        api_version: str,
        parse_kind_state: Callable = parse_kind_state_default,
        auto_include_in_watch: bool = True,
    ):
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
    def name(self) -> object:
        return self._name

    @property
    def plural(self) -> str:
        return self.name + "s"

    def parse_state(self, yaml: dict, was_deleted: bool = False) -> KubeObjectState:
        if was_deleted:
            return KubeObjectState.Deleted
        else:
            state = (self.parse_kind_state or parse_kind_state_default)(yaml)
            if not isinstance(state, KubeObjectState):
                state = KubeObjectState(state)
            return state

    def compose_resource_path(self, namespace: str, name: str = None, api_version: str = None, suffix: str = None):
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
    def create_from_existing(cls, name: str, api_version: str = None, parse_kind_state: Callable = None):
        global kinds_collection
        assert not_empty_string(name), ValueError("name cannot be null")
        name = name.lower()
        if name.lower() not in kinds_collection:
            return KubeObjectKind(name, api_version, parse_kind_state)
        global_kind = cls.get_kind(name)

        return KubeObjectKind(
            name,
            api_version or global_kind.api_version,
            parse_kind_state or global_kind.parse_kind_state,
        )

    @classmethod
    def get_kind(cls, kind: str) -> "KubeObjectKind":
        global kinds_collection
        assert isinstance(kind, str) and len(kind) > 0, ValueError("Kind must be a non empty string")
        kind = kind.lower()
        assert kind in kinds_collection, ValueError(
            f"Unknown kubernetes object kind: {kind},"
            + " you can use KubeObjectKind.register_global_kind to add new ones."
            + " (airflow_kubernetes_job_operator.kube_api.KubeObjectKind)"
        )
        return kinds_collection[kind]

    @classmethod
    def all(cls) -> List["KubeObjectKind"]:
        global kinds_collection
        return kinds_collection.values()

    @classmethod
    def watchable(cls) -> List["KubeObjectKind"]:
        return list(filter(lambda k: k.auto_include_in_watch, cls.all()))

    @classmethod
    def all_names(cls) -> List[str]:
        global kinds_collection
        return kinds_collection.keys()

    @classmethod
    def register_global_kind(cls, kind: "KubeObjectKind"):
        global kinds_collection
        kinds_collection[kind.name] = kind

    @staticmethod
    def parse_state_job(yaml: dict) -> KubeObjectState:
        status = yaml.get("status", {})
        spec = yaml.get("spec", {})
        back_off_limit = int(spec.get("backoffLimit", 1))

        job_status = KubeObjectState.Pending
        if "startTime" in status:
            if "completionTime" in status:
                job_status = KubeObjectState.Succeeded
            elif "failed" in status and int(status.get("failed", 0)) > back_off_limit:
                job_status = KubeObjectState.Failed
            else:
                job_status = KubeObjectState.Running
        return job_status

    @staticmethod
    def parse_state_pod(yaml: dict) -> KubeObjectState:
        status = yaml.get("status", {})
        pod_phase = status["phase"]
        container_status = status.get("containerStatuses", [])

        for container_status in container_status:
            if "state" in container_status:
                if (
                    "waiting" in container_status["state"]
                    and "reason" in container_status["state"]["waiting"]
                    and "BackOff" in container_status["state"]["waiting"]["reason"]
                ):
                    return KubeObjectState.Failed
                if "error" in container_status["state"]:
                    return KubeObjectState.Failed

        if pod_phase == "Pending":
            return KubeObjectState.Pending
        elif pod_phase == "Running":
            return KubeObjectState.Running
        elif pod_phase == "Succeeded":
            return KubeObjectState.Succeeded
        elif pod_phase == "Failed":
            return KubeObjectState.Failed
        return pod_phase


for kind in [
    KubeObjectKind(api_version="v1", name="Pod", parse_kind_state=KubeObjectKind.parse_state_pod),
    KubeObjectKind(api_version="v1", name="Service"),
    KubeObjectKind(api_version="v1", name="Event", auto_include_in_watch=False),
    KubeObjectKind(api_version="batch/v1", name="Job", parse_kind_state=KubeObjectKind.parse_state_job),
    KubeObjectKind(api_version="apps/v1", name="Deployment"),
]:
    KubeObjectKind.register_global_kind(kind)


class KubeObjectDescriptor:
    def __init__(
        self,
        body: dict,
        api_version: str = None,
        namespace: str = None,
        name: str = None,
        assert_metadata: bool = True,
    ):
        super().__init__()
        assert isinstance(body, dict), ValueError("body must be a dictionary")

        self._body = body
        self._kind = KubeObjectKind.create_from_existing(
            self.body.get("kind"), api_version or self.body.get("apiVersion")
        )

        if assert_metadata:
            if "metadata" not in self.body:
                self.body["metadata"] = {}

        if namespace or self.namespace:
            self.metadata["namespace"] = namespace or self.namespace
        if name or self.name:
            self.metadata["name"] = name or self.name

    @property
    def body(self) -> dict:
        return self._body

    @property
    def kind(self) -> KubeObjectKind:
        return self._kind

    @property
    def name(self) -> str:
        return self.metadata.get("name", None)

    @property
    def namespace(self) -> str:
        return self.metadata.get("namespace", None)

    @property
    def kind_plural(self) -> object:
        return self.kind.lower() + "s" if self.kind is not None else None

    @property
    def metadata(self) -> dict:
        return self.body["metadata"]

    @property
    def api_version(self) -> str:
        return self.kind.api_version

    def __str__(self):
        if self.namespace is not None:
            if self.name:
                return f"{self.namespace}/{self.kind.plural}/{self.name}"
            else:
                return f"{self.namespace}/{self.kind.plural}/{self.name}"
        else:
            return f"{self.api_version}/{self.kind.name}"
